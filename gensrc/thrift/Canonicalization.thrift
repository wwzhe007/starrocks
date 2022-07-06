struct TCanonicalOlapScanNode {
  1: required list<string> key_column_name
  2: required list<Types.TPrimitiveType> key_column_type
  3: required bool is_preaggregation
  4: optional string sort_column
  5: optional string rollup_name
  6: optional string sql_predicates
  7: optional list<i32> dict_string_ids;
  8: optional list<i32> int_ids;
  9: optional list<string> unused_output_column_name
}

struct TCanonicalProjectNode {
  1: optional list<Types.TSlotId> slot_ids;
  2: optional list<string> exprs;
  3: optional list<Types.TSlotId> cse_slot_ids;
  4: optional list<string> cse_exprs;
}

struct TCanonicalAggregationNode {
  1: optional list<string> grouping_exprs
  2: optional list<string> aggregate_functions
  3: optional bool need_finalize
  4: optional bool use_streaming_preaggregation
  5: optional i32 agg_func_set_version = 1
  6: optional bool has_outer_join_child
  7: optional i32 agg_func_set_version = 1
}

struct TDecodeNode {
    1: optional list<i32> from_dict_ids;
    2: optional list<i32> to_string_ids
    2: optional map<Types.TSlotId, Exprs.TExpr> string_functions
}
