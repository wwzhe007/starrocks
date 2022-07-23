// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/cache/lane_arbiter.h"

#include <algorithm>
#include <chrono>
#include <limits>

namespace starrocks {
namespace cache {

using std::chrono::milliseconds;
using std::chrono::steady_clock;
using std::chrono::duration_cast;

LaneArbiter::LaneArbiter(int num_lanes)
        : _passthrough_mode(false), _num_lanes(num_lanes), _assignments(num_lanes, LANE_UNASSIGNED) {}

void LaneArbiter::enable_passthrough_mode() {
    _passthrough_mode = true;
}
bool LaneArbiter::in_passthrough_mode() const {
    return _passthrough_mode;
}
bool LaneArbiter::has_free_lane() const {
    return std::any_of(_assignments.begin(), _assignments.end(),
                       [](auto& assign) { return assign == LANE_UNASSIGNED; });
}

std::optional<int> LaneArbiter::preferred_lane() const {
    int lane = -1;
    int min_assign_time = std::numeric_limits<int>::max();
    for (int i = 0; i < _assignments.size(); ++i) {
        if (_assignments[i] != LANE_UNASSIGNED && _assignments[i].assign_time < min_assign_time) {
            min_assign_time = _assignments[i].assign_time;
            lane = i;
        }
    }
    return lane == -1 ? {} : {lane};
}

int LaneArbiter::acquire_lane(size_t lane_owner) {
    auto unassigned_lane = -1;
    for (auto i = 0; i < _assignments.size(); ++i) {
        if (_assignments[i] == LANE_UNASSIGNED) {
            unassigned_lane = i;
            continue;
        }
        if (_assignments[i].lane_owner == lane_owner) {
            return i;
        }
    }
    DCHECK(unassigned_lane >= 0);
    auto assign_time = duration_cast<milliseconds>(steady_clock::now().time_since_epoch());
    _assignments[unassigned_lane] = {lane_owner, assign_time};
    return unassigned_lane;
}

void LaneArbiter::release_lane(size_t lane_owner) {
    for (auto i = 0; i < _assignments.size(); ++i) {
        if (_assignments[i].lane_owner == lane_owner) {
            _assignments[i] = LANE_UNASSIGNED;
        }
    }
}

} // namespace cache
} // namespace starrocks
