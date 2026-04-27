RAW_PRODUCTS_FIELDS = [
    "run_id",
    "sn",
    "batch_no",
    "product_model",
    "line_id",
    "equipment_id",
    "start_time",
    "end_time",
    "simulated_sort_result",
]

RAW_TEST_ITEMS_FIELDS = [
    "item_id",
    "run_id",
    "sn",
    "station_id",
    "item_name",
    "measured_value",
    "lower_limit",
    "upper_limit",
    "unit",
    "test_time",
]

RAW_STATION_EVENTS_FIELDS = [
    "event_id",
    "run_id",
    "sn",
    "station_id",
    "event_type",
    "event_message",
    "event_time",
]

PROCESSED_PRODUCT_FIELDS = [
    "run_id",
    "sn",
    "batch_no",
    "product_model",
    "line_id",
    "equipment_id",
    "start_time",
    "end_time",
    "final_result",
    "failure_codes",
    "defect_count",
    "simulated_sort_result",
]

ITEM_RESULT_FIELDS = [
    "item_id",
    "run_id",
    "sn",
    "station_id",
    "item_name",
    "measured_value",
    "lower_limit",
    "upper_limit",
    "unit",
    "test_time",
    "required",
    "item_result",
    "failure_code",
    "failure_message",
]

DEFECT_FIELDS = [
    "item_id",
    "run_id",
    "sn",
    "station_id",
    "item_name",
    "failure_code",
    "failure_message",
    "measured_value",
    "lower_limit",
    "upper_limit",
    "unit",
    "test_time",
]

DAILY_SUMMARY_FIELDS = [
    "date",
    "total_count",
    "pass_count",
    "fail_count",
    "yield_rate",
]

BATCH_SUMMARY_FIELDS = [
    "batch_no",
    "product_model",
    "total_count",
    "pass_count",
    "fail_count",
    "yield_rate",
]

DEFECT_SUMMARY_FIELDS = [
    "station_id",
    "item_name",
    "failure_code",
    "defect_count",
]
