namespace cpp starrocks
namespace java com.starrocks.thrift

include "Exprs.thrift"
include "Types.thrift"
include "Opcodes.thrift"
include "Descriptors.thrift"
include "Partitions.thrift"
include "RuntimeFilter.thrift"
include "PlanNodes.thrift"

struct TCanonicalOlapScanNode {
  1: optional list<string> key_column_name
  2: optional list<Types.TPrimitiveType> key_column_type
  3: optional bool is_preaggregation
  4: optional string sort_column
  5: optional string rollup_name
  6: optional string sql_predicates
  7: optional list<i32> dict_string_ids;
  8: optional list<i32> int_ids;
  9: optional list<string> unused_output_column_name
}

struct TCanonicalProjectNode {
  1: optional list<Types.TSlotId> slot_ids;
  2: optional list<binary> exprs;
  3: optional list<Types.TSlotId> cse_slot_ids;
  4: optional list<binary> cse_exprs;
}

struct TCanonicalAggregationNode {
  1: optional list<binary> grouping_exprs
  2: optional list<binary> aggregate_functions
  3: optional Types.TTupleId intermediate_tuple_id
  4: optional Types.TTupleId output_tuple_id
  5: optional bool need_finalize
  6: optional bool use_streaming_preaggregation
  7: optional i32 agg_func_set_version = 1
  8: optional bool has_outer_join_child
  9: optional PlanNodes.TStreamingPreaggregationMode streaming_preaggregation_mode 
}

struct TCanonicalDecodeNode {
  1: optional list<i32> from_dict_ids
  2: optional list<i32> to_string_ids
  3: optional map<Types.TSlotId, binary> string_functions
}

struct TCanonicalPlanNode {
  1: optional Types.TPlanNodeId node_id
  2: optional PlanNodes.TPlanNodeType node_type
  3: optional i32 num_children
  4: optional i64 limit
  5: optional list<Types.TTupleId> row_tuples
  6: optional list<bool> nullable_tuples
  7: optional list<binary> conjuncts

  8: optional TCanonicalAggregationNode agg_node
  9: optional TCanonicalOlapScanNode olap_scan_node
  10: optional TCanonicalProjectNode project_node
  11: optional TCanonicalDecodeNode decode_node
}
