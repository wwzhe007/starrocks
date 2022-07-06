package com.starrocks.planner;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprId;
import com.starrocks.analysis.SlotId;
import com.starrocks.common.IdGenerator;
import com.starrocks.sql.plan.ExecPlan;

import java.util.Map;

public class FragmentCanonicalizationVisitor {
    ExecPlan execPlan;
    PlanFragment fragment;
    Map<SlotId, SlotId> slotIdRemapping;
    IdGenerator<PlanNodeId> planNodeIdGen = PlanNodeId.createGenerator();
    IdGenerator<ExprId> exprIdGen = ExprId.createGenerator();
    IdGenerator<SlotId> slotIdGen = SlotId.createGenerator();
    StringBuffer canonicalString;
    Map<SlotId, SlotId> outputSlotIdRemapping;

    boolean visit(PlanNode planNode) {
        for (PlanNode child : planNode.getChildren()) {
            if (!visit(child)) {
                return false;
            }
        }
        return planNode.canonicalize(this);
    }

    boolean visit(Expr expr) {
        for (Expr child : expr.getChildren()) {
            if (!visit(child)) {
                return false;
            }
        }
        return expr.canonicalize(this);
    }
}
