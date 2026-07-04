from datetime import datetime
from summary import SummaryManager
from summary import SummaryFile
from summary import OperationInfo
import json

mgr = SummaryManager(None, None, None)

summary = SummaryFile(
    schema="repack",
    source_table="pr_index_test",
    target_table="pr_index_test_utc",
    status="COMPLETED",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    parallel_threads=4,
    startvalue=None,
    total_chunks=61,
    completed_chunks=61,
    failed_chunks=0,
    null_chunk_processed=True,
    total_rows_loaded=1074009,
    rowcount_validation=None,
    analyze_status=None,
    start_time=datetime.now(),
    end_time=datetime.now(),
    duration_seconds=11,
    operation=OperationInfo(
        type="timezone_update",
        source_timezone="America/New_York",
        target_timezone="UTC",
        updated_columns=["pxcommitdatetime", "pxcreatedatetime"],
        updated_column_count=2,
        driving_column="pxcommitdatetime",
    ),
)

mgr = SummaryManager(
    None,
    None,
    None,
    [
        "pxcommitdatetime",
        "pxcreatedatetime",
        "pxsavedatetime",
        "pxupdatedatetime",
    ],
)

operation = OperationInfo(
    type="timezone_update",
    source_timezone="America/New_York",
    target_timezone="UTC",
    updated_columns=mgr.timestamp_columns,
    updated_column_count=len(mgr.timestamp_columns),
    driving_column="pxcommitdatetime",
)

print(json.dumps(mgr.operation_to_dict(operation), indent=4))
