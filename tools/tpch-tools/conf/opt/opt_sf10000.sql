set global experimental_enable_nereids_planner=true;
set global experimental_enable_pipeline_engine=true;
set global enable_runtime_filter_prune=false;
set global runtime_filter_wait_time_ms=100000;
set global enable_fallback_to_original_planner=false;
set global forbid_unknown_col_stats=true;
set global query_timeout=1000;
set global check_overflow_for_decimal=false;