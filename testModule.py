import json

from datetime import datetime

from summary import (
    SummaryFile,
    SummaryManager,
    OperationInfo,
)

mgr = SummaryManager(
    None,
    None,
    None,
    [
        "pxcommitdatetime",
        "pxsavedatetime",
        "pxcreatedatetime",
        "pxupdatedatetime",
    ],
)

summary = SummaryFile(
    schema="repack",
    source_table="pr_index_test",
    target_table="pr_index_test_utc",
    status="COMPLETED",
    chunk_size="1M",
    parallel_threads=4,
    startvalue=None,
    total_chunks=61,
    completed_chunks=61,
    failed_chunks=0,
    null_chunk_processed=True,
    total_rows_loaded=1074009,
    rowcount_validation={
        "enabled": True,
        "source_count": 1074009,
        "target_count": 1074009,
        "status": "MATCH",
    },
    analyze_status={
        "enabled": True,
        "status": "COMPLETED",
    },
    start_time=datetime.now(),
    end_time=datetime.now(),
    duration_seconds=20,
    operation=OperationInfo(
        type="timezone_update",
        source_timezone="America/New_York",
        target_timezone="UTC",
        updated_columns=mgr.timestamp_columns,
        updated_column_count=len(mgr.timestamp_columns),
        driving_column="pxcommitdatetime",
    ),
)

print(
    json.dumps(
        mgr.to_dict(summary),
        indent=4,
    )
)
