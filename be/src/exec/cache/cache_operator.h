// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#include <shared_ptr>
#include <unordered_map>

#include "exec/cache/cache_manager.h"
#include "exec/cache/lane_arbiter.h"
#include "exec/pipeline/operator.h"
namespace starrocks {
namespace cache {
class PerLaneBuffer;
using PerLaneBufferRawPtr = PerLaneBuffer*;
using PerLaneBufferPtr = std::unique_ptr<PerLaneBuffer>;
using PerLaneBuffers = std::vector<PerLaneBufferPtr>;

class CacheOperator;
using CacheOperatorRawPtr = CacheOperator*;
using CacheOperatorPtr = std::shared_ptr<CacheOperator>;
class CacheOperator final : public pipeline::Operator {
public:
    using ChunkBufferPtr = std::shared_ptr<ChunkBuffer>;
    void probe_cache(int64_t tablet_id, int64_t version);
    void populate_cache(int64_t tablet_id);
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) override;
    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;
    Status set_finished(RuntimeState* state) override;
    Status set_finishing(RuntimeState* state) override;

private:
    bool _should_passthrough(size_t num_rows, size_t num_bytes);
    CacheManagerPtr _cache_mgr;
    const CacheKey _cache_key_prefix;
    LaneArbiterPtr _lane_arbiter;
    std::unordered_map<int64_t, size_t> _owner_to_lanes;
    PerLaneBuffers _per_lane_buffers;
    pipeline::OperatorPtr _predecessor;
    vectorized::ChunkPtr _passthrough_chunk;
    bool _is_input_finished{false};
};
} // namespace cache
} // namespace starrocks