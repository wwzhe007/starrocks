package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprId;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.IdGenerator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TCanonicalPlanNode;
import com.starrocks.thrift.TExpr;
import org.apache.commons.codec.digest.Md5Crypt;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FragmentCanonicalizationVisitor {
    ExecPlan execPlan;
    PlanFragment fragment;
    Map<PlanNodeId, PlanNodeId> planNodeIdRemapping;
    Map<SlotId, SlotId> slotIdRemapping;
    Map<TupleId, TupleId> tupleIdRemapping;
    IdGenerator<PlanNodeId> planNodeIdGen = PlanNodeId.createGenerator();
    IdGenerator<TupleId> tupleIdIdGen = TupleId.createGenerator();
    IdGenerator<ExprId> exprIdGen = ExprId.createGenerator();
    IdGenerator<SlotId> slotIdGen = SlotId.createGenerator();
    StringBuffer canonicalString;
    Map<SlotId, SlotId> outputSlotIdRemapping;
    List<TCanonicalPlanNode> canonicalizedPlanNodes;
    PlanNode topmostPlanNode;

    public List<Integer> remapTupleIds(List<TupleId> ids) {
        return ids.stream().map(id -> remapTupleId(id).asInt()).collect(Collectors.toList());
    }

    public PlanNodeId remapPlanNodeId(PlanNodeId planNodeId) {
        return planNodeIdRemapping.computeIfAbsent(planNodeId, arg -> planNodeIdGen.getNextId());
    }

    public SlotId remapSlotId(SlotId slotId) {
        return slotIdRemapping.computeIfAbsent(slotId, arg -> slotIdGen.getNextId());
    }

    public TupleId remapTupleId(TupleId tupleId) {
        return tupleIdRemapping.computeIfAbsent(tupleId, arg -> tupleIdIdGen.getNextId());
    }

    public ByteBuffer canonicalizeExpr(Expr expr) {
        TExpr texpr = expr.canonicalize(this);
        TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
        try {
            return ByteBuffer.wrap(ser.serialize(texpr));
        } catch (Exception ignored) {
            Preconditions.checkArgument(false);
        }
        return null;
    }

    public List<ByteBuffer> canonicalizeExprs(List<Expr> exprList) {
        List<ByteBuffer> byteStrings = exprList.stream().map(this::canonicalizeExpr).collect(Collectors.toList());

        byteStrings.sort((lhs, rhs) -> {
            byte[] lhsBytes = lhs.array();
            byte[] rhsBytes = rhs.array();
            int minLen = Math.min(lhsBytes.length, rhsBytes.length);
            for (int i = 0; i < minLen; ++i) {
                int r = lhsBytes[i] - rhsBytes[i];
                if (r != 0) {
                    return Integer.signum(r);
                }
                continue;
            }
            return Integer.signum(lhsBytes.length - rhsBytes.length);
        });
        return byteStrings;
    }

    boolean isCanonicalizable(PlanNode node) {
        Preconditions.checkArgument(node != null);
        return node instanceof OlapScanNode ||
                node instanceof ProjectNode ||
                node instanceof SelectNode ||
                node instanceof AggregationNode ||
                node instanceof DecodeNode;
    }

    public void canonicalize() throws TException {
        topmostPlanNode = findMaximumCanonicalizableSubTree(fragment.getPlanRoot());
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        Md5Crypt.md5Crypt()
        for (TCanonicalPlanNode node: canonicalizedPlanNodes) {
            byte[] data = serializer.serialize(node);
        }
        canonicalizedPlanNodes
    }

    public PlanNode findMaximumCanonicalizableSubTree(PlanNode node) {
        boolean allCanonicalized = true;
        PlanNode leftMostTree = null;
        for (PlanNode child : node.getChildren()) {
            PlanNode subtree = findMaximumCanonicalizableSubTree(child);
            if (subtree == null) {
                allCanonicalized = false;
                continue;
            }
            if (subtree != child) {
                return child;
            }
            if (leftMostTree == null) {
                leftMostTree = subtree;
            }
        }

        if (!allCanonicalized) {
            return null;
        }

        if (!isCanonicalizable(node)) {
            return leftMostTree;
        } else {
            TCanonicalPlanNode canonNode = node.canonicalize(this);
            canonicalizedPlanNodes.add(canonNode);
            return node;
        }
    }
}
