// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
#pragma once

#include <shared_ptr>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/cache/lane_arbiter.h"
#include "exec/pipeline/operator.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace cache {
class MultilaneOperator;
using MultilaneOperatorRawPtr = MultilaneOperator*;
using MultilaneOperatorPtr = std::shared_ptr<MultilaneOperator>;
class MultilaneOperatorFactory;
using MultilaneOperatorFactorRawPtr = MultilaneOperatorFactory*;
using MultilaneOperatorFactoryPtr = std::shared_ptr<MultilaneOperatorFactory>;

class MultilaneOperator final: public pipeline::Operator {
public:
    struct Lane {
        pipeline::OperatorPtr processor;
        int64_t lane_owner;
        int lane_id;
        bool last_chunk_received;
    };

    MultilaneOperator(pipeline::Operators&& processors);
    ~MultilaneOperator() = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;
    Status set_cancelled(RuntimeState* state) override;
    Status set_precondition_ready(RuntimeState* state) override;
    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;
    bool pending_finish() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk);

private:
    std::unordered_map<int64_t, int> _owner_to_lanes;
    std::vector<Lane> _lanes;
    LaneArbiterPtr _lane_arbiter;
    vectorized::ChunkPtr _passthrough_chunk;
    bool _input_finished{false};
};
class MultilaneOperatorFactory final:public pipeline::OperatorFactory{
public:
    MultilaneOperatorFactory(int num_lanes, OperatorFactoryPtr factory);
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;
    Status prepare(RuntimeState *state) override;
    void close(RuntimeState *state) override;
private:
    int _num_lanes;
    OperatorFactoryPtr _factory;
};
} // namespace cache
} // namespace starrocks