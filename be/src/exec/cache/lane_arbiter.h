// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once
#include <atomic>
#include <memory>
#include <vector>
#include <optional>
#include <unordered_map>
namespace starrocks {
namespace cache {
class LaneArbiter;
using LaneArbiterRawPtr = LaneArbiter*;
using LaneArbiterPtr = std::shared_ptr<LaneArbiter>;

class LaneArbiter {
public:
    struct LaneAssignment {
        int64_t lane_owner;
        int64_t assign_time;
    };
    static constexpr LaneAssignment LANE_UNASSIGNED = {-1, -1};
    LaneArbiter(int num_lanes);
    ~LaneArbiter() = default;
    void enable_passthrough_mode();
    bool in_passthrough_mode() const;
    bool has_free_lane() const;
    int acquire_lane(size_t lane_owner);
    void release_lane(size_t lane_owner);
    std::optional<int> preferred_lane() const;

private:
    LaneArbiter(const LaneArbiter&) = delete;
    LaneArbiter& operator=(const LaneArbiter&) = delete;
    std::atomic<bool> _passthrough_mode;
    const size_t _num_lanes;
    std::vector<LaneAssignment> _assignments;
};

} // namespace cache
} // namespace starrocks