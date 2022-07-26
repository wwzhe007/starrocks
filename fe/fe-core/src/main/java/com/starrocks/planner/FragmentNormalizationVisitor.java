package com.starrocks.planner;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprId;
import com.starrocks.analysis.SlotId;
import com.starrocks.analysis.TupleId;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.Pair;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TNormalPlanNode;
import com.starrocks.thrift.TExpr;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FragmentNormalizationVisitor {
    ExecPlan execPlan;
    PlanFragment fragment;
    Map<PlanNodeId, PlanNodeId> planNodeIdRemapping;
    Map<SlotId, SlotId> slotIdRemapping;
    Map<TupleId, TupleId> tupleIdRemapping;
    IdGenerator<PlanNodeId> planNodeIdGen = PlanNodeId.createGenerator();
    IdGenerator<TupleId> tupleIdIdGen = TupleId.createGenerator();
    IdGenerator<ExprId> exprIdGen = ExprId.createGenerator();
    IdGenerator<SlotId> slotIdGen = SlotId.createGenerator();
    ByteBuffer normalizedDigest;
    Map<SlotId, SlotId> outputSlotIdRemapping;
    List<TNormalPlanNode> normalizedPlanNodes;
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

    public List<Integer> remapSlotIds(List<SlotId> slotIds) {
        return slotIds.stream().map(this::remapSlotId).map(SlotId::asInt).collect(Collectors.toList());
    }

    public TupleId remapTupleId(TupleId tupleId) {
        return tupleIdRemapping.computeIfAbsent(tupleId, arg -> tupleIdIdGen.getNextId());
    }

    public ByteBuffer normalizeExpr(Expr expr) {
        TExpr texpr = expr.normalize(this);
        TSerializer ser = new TSerializer(new TCompactProtocol.Factory());
        try {
            return ByteBuffer.wrap(ser.serialize(texpr));
        } catch (Exception ignored) {
            Preconditions.checkArgument(false);
        }
        return null;
    }

    public Pair<List<Integer>, List<ByteBuffer>> normalizeSlotIdsAndExprs(Map<SlotId, Expr> exprMap) {
        List<Pair<SlotId, ByteBuffer>> slotIdsAndStringFunctions = exprMap.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), normalizeExpr(e.getValue())))
                .sorted(Pair.comparingBySecond()).collect(Collectors.toList());
        List<SlotId> slotIds = slotIdsAndStringFunctions.stream().map(e -> e.first).collect(Collectors.toList());
        List<ByteBuffer> exprs = slotIdsAndStringFunctions.stream().map(e -> e.second).collect(Collectors.toList());
        return new Pair<>(remapSlotIds(slotIds), exprs);
    }

    public List<ByteBuffer> normalizeExprs(List<Expr> exprList) {
        if (exprList == null || exprList.isEmpty()) {
            return Collections.emptyList();
        }
        return exprList.stream().map(this::normalizeExpr).sorted(ByteBuffer::compareTo).collect(Collectors.toList());
    }

    boolean isNormalizable(PlanNode node) {
        Preconditions.checkArgument(node != null);
        return node instanceof OlapScanNode ||
                node instanceof ProjectNode ||
                node instanceof SelectNode ||
                node instanceof AggregationNode ||
                node instanceof DecodeNode;
    }

    public void normalize() throws TException, NoSuchAlgorithmException {
        topmostPlanNode = findMaximumNormalizableSubTree(fragment.getPlanRoot());
        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (TNormalPlanNode node : normalizedPlanNodes) {
            byte[] data = serializer.serialize(node);
            digest.update(data);
        }
        normalizedDigest = ByteBuffer.wrap(digest.digest());
        List<SlotId> slotIds = topmostPlanNode.getOutputSlotIds(execPlan.getDescTbl());
        List<Integer> remappedSlotIds = remapSlotIds(slotIds);
        for (int i = 0; i < slotIds.size(); ++i) {
            outputSlotIdRemapping.put(slotIds.get(i), new SlotId(remappedSlotIds.get(i)));
        }
    }

    public PlanNode findMaximumNormalizableSubTree(PlanNode node) {
        boolean allNormalized = true;
        PlanNode leftMostTree = null;
        for (PlanNode child : node.getChildren()) {
            PlanNode subtree = findMaximumNormalizableSubTree(child);
            if (subtree == null) {
                allNormalized = false;
                continue;
            }
            if (subtree != child) {
                return child;
            }
            if (leftMostTree == null) {
                leftMostTree = subtree;
            }
        }

        if (!allNormalized) {
            return null;
        }

        if (!isNormalizable(node)) {
            return leftMostTree;
        } else {
            TNormalPlanNode canonNode = node.normalize(this);
            normalizedPlanNodes.add(canonNode);
            return node;
        }
    }
}
