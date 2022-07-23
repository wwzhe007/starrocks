#include "exec/cache/multilane_operator.h"

namespace starrocks {
namespace cache {
bool MultilaneOperator::need_input() const {
    if (is_finished()) {
        return false;
    }

    if (_lane_arbiter->in_passthrough_mode()) {
        return _passthrough_chunk == nullptr;
    }

    for (auto it : _owner_to_lanes) {
        auto lane_id = it.second;
        auto& lane = _lanes[lane_id];
        if (lane.processor->need_input()) {
            return true;
        }
    }
    return _owner_to_lanes.size() < _lanes.size();
}
bool MultilaneOperator::has_output() const {
    for (auto it : _owner_to_lanes) {
        auto lane_id = it.second;
        auto& lane = _lanes[lane_id];
        if (!lane.processor->is_finished() && lane.processor->has_output()) {
            return true;
        }
    }
    if (_lane_arbiter->in_passthrough_mode()) {
        return _passthrough_chunk != nullptr;
    }
    return false;
}

bool MultilaneOperator::is_finished() const {
    if (!_input_finished) {
        return false;
    }
    auto all_lanes_finished = std::all_of(_owner_to_lanes.begin(), _owner_to_lanes.end(),
                                          [this](auto it) { return _lanes[it->second]->is_finished(); });

    return all_lanes_finished && _passthrough_chunk == nullptr;
}

Status MultilaneOperator::set_finishing(RuntimeState* state) {
    _input_finished = true;
    for (auto it : _owner_to_lanes) {
        auto& lane = _lanes[it.second];
        lane.processor->set_finishing(state);
    }
}

Status MultilaneOperator::set_finished(RuntimeState* state) {
    _input_finished = true;
    for (auto it : _owner_to_lanes) {
        auto& lane = _lanes[it.second];
        lane.processor->set_finished(state);
    }
}

Status MultilaneOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    DCHECK(chunk != nullptr);
    if (_lane_arbiter->in_passthrough_mode()) {
        _passthrough_chunk = std::move(chunk);
        return Status::OK();
    }
    auto lane_owner = chunk->tablet_id();
    auto is_last_chunk = chunk->is_last_chunk();

    if (!_owner_to_lanes.count(lane_owner)) {
        auto lane_id = _lane_arbiter->acquire_lane(lane_owner);
        auto& lane = _lanes[lane_id];
        lane.lane_id = lane_id;
        lane.lane_owner = lane_owner;
        lane.processor->reset_state({});
    }
    auto& lane = _lanes[_owner_to_lanes[lane_owner]];
    lane.last_chunk_received = is_last_chunk;
    auto status = lane.processor->push_chunk(state, chunk);
    if (is_last_chunk) {
        lane.processor->set_finishing(state);
    }
    return status;
}

StatusOr<vectorized::ChunkPtr> MultilaneOperator::pull_chunk(RuntimeState* state) {
    auto pull_chunk_from_lane = [](Lane & lane) -> auto {
        if (lane.processor->has_output()) {
            DCHECK(!lane.processor->is_finished());
            auto chunk = lane.processor->pull_chunk(state);
            if (chunk.ok() && chunk.value() != nullptr) {
                chunk.value()->set_tablet_id(lane.lane_owner, lane.processor->is_finished());
                return chunk;
            }
        }
        return nullptr;
    };

    auto preferred = _lane_arbiter->preferred_lane();
    if (preferred.has_value()) {
        auto& lane = _lanes[preferred.value()];
        auto chunk = pull_chunk_from_lane(lane);
        if (!chunk.ok() || chunk.value() != nullptr) {
            return chunk;
        }
    }

    for (int i = 0; i < _lanes.size(); ++i) {
        auto& lane = _lanes[i];
        auto chunk = pull_chunk_from_lane(lane);
        if (!chunk.ok() || chunk.value() != nullptr) {
            return chunk;
        }
    }

    if (_lane_arbiter->in_passthrough_mode()) {
        return std::move(_passthrough_chunk);
    } else {
        return nullptr;
    }
}

MultilaneOperatorFactory::MultilaneOperatorFactory(int num_lanes, OperatorFactoryPtr factory)
        : _num_lanes(num_lanes), _factory(factory) {}

pipeline::OperatorPtr MultilaneOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence)override;
Status MultilaneOperatorFactory::prepare(RuntimeState* state)override;
void MultilaneOperatorFactory::close(RuntimeState* state)override;

} // namespace cache
} // namespace starrocks