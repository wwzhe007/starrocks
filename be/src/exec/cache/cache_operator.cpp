// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include "exec/cache/cache_operator.h"

#include <vector>

#include "common/compiler_util.h"
#include "storage/rowset/rowset.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/time.h"
namespace starrocks {
namespace cache {
enum PerLaneBufferState {
    PLBS_INIT,
    PLBS_MISS,
    PLBS_HIT_PARTIAL,
    PLBS_HIT_TOTAL,
    PLBS_PARTIAL,
    PLBS_TOTAL,
    PLBS_POPULATE,
    PLBS_PASSTHROUGH,
};
struct PerLaneBuffer {
    int lane;
    PerLaneBufferState state;
    TabletSharedPtr tablet;
    std::vector<RowsetSharedPtr> rowsets;
    int64_t required_version;
    int64_t cached_version;
    std::vector<ChunkPtr> chunks;
    size_t next_gc_chunk_idx;
    size_t next_chunk_idx;
    size_t num_rows;
    size_t num_bytes;

    void clear() {
        lane = -1;
        state = PLBS_INIT;
        tablet.reset();
        rowsets.clear();
        required_version = 0;
        cached_version = 0;
        chunks.clear();
        next_gc_chunk_id = 0;
        net_chunk_idx = 0;
        num_rows = 0;
        num_bytes = 0;
    }
    bool should_populate_cache() const {
        return cached_version < required_version && (state == PLBS_HIT_TOTAL || state == PLBS_TOTAL);
    }
    bool is_partial() const { return state == PLBS_HIT_PARTIAL || state == PLBS_PARTIAL; }
    void append_chunk(const vectorized::ChunkPtr& chunk) {
        DCHECK(is_partial());
        chunks.push_back(chunk);
        state = chunk->is_last_chunk() ? PLBS_TOTAL : PLBS_PARTIAL;
        num_rows += chunk->num_rows();
        num_bytes += chunk->bytes_usage();
    }
    bool has_chunks() const { return next_chunk_idx < chunks.size(); }

    void set_passthrough() {
        if (is_partial()) {
            state = PLBS_PASSTHROUGH;
        }
    }

    void can_release() { return (state == PLBS_POPULATE || state == PLBS_PASSTHROUGH) && !has_chunks(); }

    ChunkPtr get_next_chunk() {
        if (UNLIKELY(state == PLBS_POPULATE || state == PLBS_PASSTHROUGH)) {
            while (UNLIKELY(next_gc_chunk_idx < next_chunk_idx)) {
                chunks[next_gc_chunk_idx++].reset();
            }
            ++next_gc_chunk_idx;
            return std::move(chunks[next_chunk_idx++]);
        } else {
            return _chunks[next_chunk_idx++];
        }
    }
};

void CacheOperator::probe_cache(int64_t tablet_id, int64_t version) {
    // allocate lane and PerLaneBuffer for tablet_id
    int64_t lane = _lane_arbiter->acquire_lane(tablet_id);
    _owner_to_lanes[tablet_id] = lane;
    auto& buffer = _per_lane_buffers[lane];
    buffer->clear();
    buffer->lane = lane;
    buffer->required_version = version;
    // probe cache
    CacheKey key{Slice(_cache_key_prefix), tablet_id};
    auto status = _cache_mgr->probe(key);

    // Cache MISS when failed to probe
    if (!status.ok()) {
        buffer->state = PLBS_MISS;
        return;
    }

    auto& cache_value = status.value();
    if (cache_value.version == version) {
        // Cache HIT_TOTAL when cached version equals to required version
        buffer->state = PLBS_HIT_TOTAL;
        buffer->cached_version = cache_value.version;
        buffer->chunks = std::move(cache_value.result);
    } else if (cache_value.version > version) {
        // It rarely happens that required version is less that cached version, the required version become
        // stale when the query is postponed to be processed because of some reasons, for examples, non-deterministic
        // query scheduling, network congestion etc. make queries be executed out-of-order. so we must prevent stale
        // result from replacing fresh cached result.
        buffer->state = PLBS_MISS;
        buffer->cached_version = 0;
    } else {
        // Try to reuse partial cache result when cached version is less than required version, delta versions
        // should be captured at first.
        auto status = StorageEngine::instance()->tablet_manager()->capture_tablet_and_rowsets(
                tablet_id, cache_value.version + 1, version);
        if (!status.ok()) {
            // Cache MISS if delta versions are not captured, because aggressive cumulative compactions.
            buffer->state = PLBS_MISS;
            buffer->version = 0;
        } else {
            // Delta versions are captured, several situations should be taken into consideration.
            // case 1: all delta versions are empty rowsets, so the cache result is hit totally.
            // case 2: there exist delete predicates in delta versions, the cache result is not reuse, so cache miss.
            // case 3: otherwise, the cache result is partial result of per-tablet computation, so delta versions must
            //  be scanned and merged with cache result to generate total result.
            auto& [tablet, rowsets] = status.value();
            auto all_rs_empty = true;
            auto min_version = std::numeric_limits<int64_t>::max();
            auto max_version = std::numeric_limits<int64_t>::min();
            for (const auto& rs : rowsets) {
                all_rs_empty &= !rs->has_data_files();
                min_version = std::min(min_version, rs->start_version());
                max_version = std::max(max_version, rs->end_version());
            }
            Version version(min_version, max_version);
            buffer->tablet = std::move(tablet);
            auto has_delete_predicates = tablet->has_delete_predicates(version);
            if (has_delete_predicates) {
                buffer->state = PLBS_MISS;
                buffer->cached_version = 0;
                return;
            }

            buffer->cached_version = cache_value.version;
            buffer->chunks = std::move(cache_value.result);
            if (all_rs_empty) {
                buffer->state = PLBS_HIT_TOTAL;
            } else {
                buffer->state = PLBS_HIT_PARTIAL;
                buffer->rowsets = std::move(rowsets);
            }
        }
    }
}
void CacheOperator::populate_cache(int64_t tablet_id) {
    DCHECK(_owner_to_lanes.count(table_id));
    auto lane = _owner_to_lanes[tablet_id];
    auto& buffer = _per_lane_buffers[lane];
    if (!buffer->should_populate_cache()) {
        return;
    }
    CacheKey key{_cache_key_prefix, tablet_id};
    int64_t current = GetMonoTimeMicros();
    CacheValue value{0, 0, current, buffer->required_version, buffer->chunks};
    // If the cache implementation is global, populate method must be asynchronous and try its best to
    // update the cache.
    _cache_mgr->populate(key, value);
    buffer->state = PLBS_POPULATE;
}

Status CacheOperator::set_finishing(RuntimeState* state) {
    _is_input_finished = true;
    return Status::OK();
}

Status CacheOperator::set_finished(RuntimeState* state) {
    _is_input_finished = true;
    for (auto& buffer : _per_lane_buffers) {
        buffer->clear();
    }
    _passthrough_chunk.reset();
    DCHECK(is_finished());
    return Status::OK();
}

bool CacheOperator::is_finished() const {
    return _is_input_finished && !has_output();
}

bool CacheOperator::has_output() const {
    for (auto it : _owner_to_lanes) {
        auto lane = it.second;
        auto& buffer = _per_lane_buffers[lane];
        if (buffer->has_chunks()) {
            return true;
        }
    }
    return _passthrough_chunk != nullptr;
}

Status CacheOperator::need_input() const {
    return !_is_input_finished && (!_lane_arbiter->in_passthrough_mode() || _passthrough_chunk == nullptr);
}

Status CacheOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    DCHECK(chunk != nullptr);
    if (_lane_arbiter->in_passthrough_mode()) {
        DCHECK(_passthrough_chunk == nullptr);
        _passthrough_chunk = chunk;
        return Status::OK();
    }
    auto lane_owner = chunk->tablet_id();
    auto is_last_chunk = chunk->is_last_chunk();
    DCHECK(_owner_to_lanes.count(lane_owner));
    auto lane = _owner_to_lanes[lane_owner];
    DCHECK(_per_lane_buffers.count(lane));
    auto& buffer = _per_lane_buffers[lane];
    buffer->append_chunk(chunk);

    if (_should_passthrough(buffer->num_rows, buffer->num_bytes)) {
        _lane_arbiter->enable_passthrough_mode();
        for (auto it : _owner_to_lanes) {
            auto lane = it.second;
            _per_lane_buffers[lane]->set_passthrough();
        }
        return Status::OK();
    }

    if (buffer->should_populate_cache()) {
        populate_cache(lane_owner);
    }
    return Status::OK();
}
StatusOr<vectorized::ChunkPtr> CacheOperator::pull_chunk(RuntimeState* state) {
    auto opt_lane = _lane_arbiter->preferred_lane();
    if (opt_lane.has_value()) {
        auto lane = opt_lane.value();
        auto& buffer = _per_lane_buffers[lane];
        if (buffer->has_chunks()) {
            auto chunk = buffer->get_next_chunk();
            if (buffer->can_release()) {
                _lane_arbiter->release_lane(chunk->tablet_id());
                buffer->clear();
            }
            return chunk;
        }
    }

    for (auto it : _owner_to_lanes) {
        auto lane = it.second;
        auto& buffer = _per_lane_buffers[lane];
        if (buffer->has_chunks()) {
            auto chunk = buffer->get_next_chunk();
            if (buffer->can_release()) {
                _lane_arbiter->release_lane(chunk->tablet_id());
                buffer->clear();
            }
            return chunk;
        }
    }
    if (_lane_arbiter->in_passthrough_mode()) {
        return std::move(_passthrough_chunk);
    } else {
        return nullptr;
    }
}
} // namespace cache
} // namespace starrocks
