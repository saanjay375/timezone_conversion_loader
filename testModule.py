import json
import os
import sys
import tempfile
import types
from contextlib import contextmanager
from datetime import datetime

metadata_stub = types.ModuleType("metadata")
metadata_stub.MetadataRepository = object
metadata_stub.get_connection = None
sys.modules["metadata"] = metadata_stub

paths_stub = types.ModuleType("paths")
paths_stub.get_summary_file = lambda schema, table: "summary.json"
sys.modules["paths"] = paths_stub

from config import (
    ConfigLoader,
    DatabaseConfig,
    GlobalConfig,
    OperationConfig,
    TableConfig,
)
from processor import CheckpointManager, ChunkProcessor, StatisticsCollector
from sql_generator import SQLGenerator
from summary import SummaryManager


class Chunk:
    chunk_number = 1
    start_value = datetime(2025, 1, 1)
    end_value = datetime(2025, 2, 1)
    predicate = "range"
    total_chunks = 1
    is_null_chunk = False


class Cursor:
    rowcount = 100

    def __init__(self, validation_row):
        self.validation_row = validation_row

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql_text, values=None):
        pass

    def fetchone(self):
        return self.validation_row


class Connection:
    def __init__(self, validation_row):
        self.validation_row = validation_row

    def cursor(self):
        return Cursor(self.validation_row)

    def commit(self):
        pass


class Repository:
    def __init__(self, connection):
        pass

    def configure_session(self, statement_timeout_ms, lock_timeout_ms):
        pass


class Logger:
    def info(self, message, *args):
        print(message % args if args else message)

    def error(self, message, *args):
        print(message % args if args else message)


class SQL:
    table_context = {
        "timestamp_columns": [],
        "lob_columns": ["message_text", "payload_bytes"],
    }

    def build_range_insert_sql(self):
        return "INSERT"

    def build_null_chunk_sql(self):
        return "INSERT NULL"

    def build_range_rowcount_lob_validation_sql(self):
        return "VALIDATE ROWCOUNT LOB"

    def build_null_rowcount_lob_validation_sql(self):
        return "VALIDATE NULL ROWCOUNT LOB"


class CaptureLogger:

    def __init__(self):
        self.messages = []

    def info(self, message, *args):
        if args:
            message = message % args
        self.messages.append(message)

    def error(self, message, *args):
        pass


db = DatabaseConfig("localhost", 5432, "test", "test", "test")
global_config = GlobalConfig(database=db)
operation = OperationConfig(
    type="timezone_update",
    source_timezone="America/New_York",
    target_timezone="UTC",
    target_table_suffix="_utc",
)
table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    rowcount_lob_validation=True,
)

import processor

processor.MetadataRepository = Repository

# source_count, target_count, then four metrics per LOB column.
PASSING_ROW = (
    100,
    100,
    250,
    10000,
    250,
    10000,
    4096,
    250000,
    4096,
    250000,
)


@contextmanager
def passing_connection(*args, **kwargs):
    yield Connection(PASSING_ROW)


with tempfile.TemporaryDirectory() as temp:
    processor.get_connection = passing_connection
    checkpoint = CheckpointManager(os.path.join(temp, "checkpoint.json"))
    stats = StatisticsCollector()
    worker = ChunkProcessor(
        global_config, operation, table, Logger(), SQL(), checkpoint, stats
    )
    result = worker.process_chunk(Chunk())

    assert result.success is True
    assert result.rowcount_lob_validation.status == "PASSED"
    assert result.rowcount_lob_validation.source_count == 100
    assert result.rowcount_lob_validation.target_count == 100
    assert result.rowcount_lob_validation.lob_columns_validated == 2
    assert checkpoint.load()["resume_watermark_chunk"] == 1

    manager = SummaryManager(global_config, operation, table, Logger(), [])
    summary = manager.build_summary(
        stats,
        1,
        datetime(2025, 1, 1),
        datetime(2025, 1, 1, 0, 0, 5),
    )
    payload = manager.to_dict(summary)
    validation = payload["validation"]["rowcount_lob_validation"]
    assert validation["status"] == "PASSED"
    assert validation["rows_validated"] == 100
    assert validation["lob_columns_validated"] == 2

print("CHANGE-6D PASS PATH TEST PASSED")

FAILING_ROW = (
    100,
    99,
    250,
    10000,
    250,
    9999,
    4096,
    250000,
    4096,
    250000,
)


@contextmanager
def failing_connection(*args, **kwargs):
    yield Connection(FAILING_ROW)


with tempfile.TemporaryDirectory() as temp:
    processor.get_connection = failing_connection
    checkpoint = CheckpointManager(os.path.join(temp, "checkpoint.json"))
    stats = StatisticsCollector()
    worker = ChunkProcessor(
        global_config, operation, table, Logger(), SQL(), checkpoint, stats
    )
    result = worker.process_chunk(Chunk())

    assert result.success is False
    assert stats.failed_chunks == 1
    assert checkpoint.load() is None

print("CHANGE-6D FAILURE BLOCKS WATERMARK TEST PASSED")

sql = SQLGenerator(
    global_config,
    operation,
    table,
    {
        "all_columns": [
            "id",
            "pxcommitdatetime",
            "message_text",
            "payload_bytes",
        ],
        "timestamp_columns": ["pxcommitdatetime"],
        "primary_key_columns": ["id"],
        "lob_columns": ["message_text", "payload_bytes"],
    },
).build_range_rowcount_lob_validation_sql()

assert "COUNT(*) AS source_count" in sql
assert "target_count" in sql
assert 'octet_length(s."message_text")' in sql
assert 'octet_length(t."payload_bytes")' in sql
assert 's."pxcommitdatetime" >= %s' in sql
print("CHANGE-6D SQL GENERATION TEST PASSED")


def load_test_config(table_overrides=None):
    table_config = {
        "schema": "repack",
        "table_name": "pr_index_test",
        "driving_column": "pxcommitdatetime",
        "chunk_size": "1M",
    }

    if table_overrides:
        table_config.update(table_overrides)

    config_payload = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "dbname": "test",
            "username": "test",
            "password": "test",
        },
        "operations": [
            {
                "type": "timezone_update",
                "source_timezone": "America/New_York",
                "target_timezone": "UTC",
                "target_table_suffix": "_utc",
                "tables": [table_config],
            }
        ],
    }

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False,
        encoding="utf-8",
    ) as handle:
        json.dump(config_payload, handle)
        config_file = handle.name

    try:
        return ConfigLoader.load(config_file)
    finally:
        os.remove(config_file)


explicit_config = load_test_config(
    {
        "validation_fail_on_error": True,
        "validation_log_details": False,
    }
)
explicit_table = explicit_config.operations[0].tables[0]

assert explicit_table.validation_fail_on_error is True
assert explicit_table.validation_log_details is False
print("CHANGE-6E-A EXPLICIT CONFIG TEST PASSED")


default_config = load_test_config()
default_table = default_config.operations[0].tables[0]

assert default_table.validation_fail_on_error is False
assert default_table.validation_log_details is True
print("CHANGE-6E-A DEFAULT CONFIG TEST PASSED")

backward_compatible_config = load_test_config(
    {
        "timestamp_update_validation": True,
        "rowcount_lob_validation": True,
        "validate_rowcount": True,
        "analyze_after_load": True,
    }
)
backward_compatible_table = backward_compatible_config.operations[0].tables[0]

assert backward_compatible_table.timestamp_update_validation is True
assert backward_compatible_table.rowcount_lob_validation is True
assert backward_compatible_table.validate_rowcount is True
assert backward_compatible_table.analyze_after_load is True
assert backward_compatible_table.validation_fail_on_error is False
assert backward_compatible_table.validation_log_details is True
print("CHANGE-6E-A BACKWARD COMPATIBILITY TEST PASSED")

print("ALL CHANGE-6D AND CHANGE-6E-A TESTS PASSED")

# ------------------------------------------------------------------
# CHANGE-6E-B: VALIDATION RESULT AGGREGATION TESTS
# ------------------------------------------------------------------

# Test 1: PASSED + PASSED => PASSED
aggregation_stats = StatisticsCollector()
aggregation_stats.timestamp_chunks_validated = 1
aggregation_stats.timestamp_rows_validated = 100
aggregation_stats.timestamp_columns_validated = 1
aggregation_stats.timestamp_validation_duration_seconds = 2
aggregation_stats.rowcount_lob_chunks_validated = 1
aggregation_stats.rowcount_lob_rows_validated = 100
aggregation_stats.lob_columns_validated = 2
aggregation_stats.rowcount_lob_validation_duration_seconds = 3

aggregate = aggregation_stats.build_validation_summary(True, True)

assert aggregate.overall_status == "PASSED"
assert aggregate.timestamp_update_validation_status == "PASSED"
assert aggregate.rowcount_lob_validation_status == "PASSED"
assert aggregate.rows_validated == 200
assert aggregate.mismatch_count == 0
assert aggregate.timestamp_columns_validated == 1
assert aggregate.lob_columns_validated == 2
assert aggregate.duration_seconds == 5

print("CHANGE-6E-B PASSED AGGREGATION TEST PASSED")

# Test 2: FAILED + PASSED => FAILED
aggregation_stats = StatisticsCollector()
aggregation_stats.timestamp_chunks_validated = 1
aggregation_stats.timestamp_rows_validated = 100
aggregation_stats.timestamp_mismatch_count = 5
aggregation_stats.timestamp_columns_validated = 1
aggregation_stats.rowcount_lob_chunks_validated = 1
aggregation_stats.rowcount_lob_rows_validated = 100
aggregation_stats.lob_columns_validated = 2

aggregate = aggregation_stats.build_validation_summary(True, True)

assert aggregate.overall_status == "FAILED"
assert aggregate.timestamp_update_validation_status == "FAILED"
assert aggregate.rowcount_lob_validation_status == "PASSED"
assert aggregate.rows_validated == 200
assert aggregate.mismatch_count == 5

print("CHANGE-6E-B FAILED AGGREGATION TEST PASSED")

# Test 3: Enabled but no validation completed => NOT_RUN
aggregation_stats = StatisticsCollector()
aggregate = aggregation_stats.build_validation_summary(True, True)

assert aggregate.overall_status == "NOT_RUN"
assert aggregate.timestamp_update_validation_status == "NOT_RUN"
assert aggregate.rowcount_lob_validation_status == "NOT_RUN"

print("CHANGE-6E-B NOT-RUN AGGREGATION TEST PASSED")

# Test 4: Both validation types disabled => DISABLED
aggregation_stats = StatisticsCollector()
aggregate = aggregation_stats.build_validation_summary(False, False)

assert aggregate.overall_status == "DISABLED"
assert aggregate.timestamp_update_validation_status == "DISABLED"
assert aggregate.rowcount_lob_validation_status == "DISABLED"
assert aggregate.rows_validated == 0
assert aggregate.mismatch_count == 0

print("CHANGE-6E-B DISABLED AGGREGATION TEST PASSED")

# Test 5: One validation passed and the other disabled => PASSED
aggregation_stats = StatisticsCollector()
aggregation_stats.rowcount_lob_chunks_validated = 1
aggregation_stats.rowcount_lob_rows_validated = 100
aggregation_stats.lob_columns_validated = 2

aggregate = aggregation_stats.build_validation_summary(False, True)

assert aggregate.overall_status == "PASSED"
assert aggregate.timestamp_update_validation_status == "DISABLED"
assert aggregate.rowcount_lob_validation_status == "PASSED"

print("CHANGE-6E-B PARTIAL ENABLEMENT AGGREGATION TEST PASSED")

# Test 6: Summary serialization contains aggregate and component results
aggregation_table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    timestamp_update_validation=True,
    rowcount_lob_validation=True,
)

aggregation_stats = StatisticsCollector()
aggregation_stats.timestamp_chunks_validated = 1
aggregation_stats.timestamp_rows_validated = 100
aggregation_stats.timestamp_columns_validated = 1
aggregation_stats.timestamp_validation_duration_seconds = 2
aggregation_stats.rowcount_lob_chunks_validated = 1
aggregation_stats.rowcount_lob_rows_validated = 100
aggregation_stats.lob_columns_validated = 2
aggregation_stats.rowcount_lob_validation_duration_seconds = 3

# ------------------------------------------------------------------
# CHANGE-6E-C-1: VALIDATION REPORTING METRICS
# ------------------------------------------------------------------

aggregation_table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    timestamp_update_validation=True,
    rowcount_lob_validation=True,
)

# ----------------------------------------------------------
# TEST 1
# Both validations PASSED
# ----------------------------------------------------------

stats = StatisticsCollector()

stats.timestamp_chunks_validated = 1
stats.timestamp_rows_validated = 100
stats.timestamp_columns_validated = 1

stats.rowcount_lob_chunks_validated = 1
stats.rowcount_lob_rows_validated = 100
stats.lob_columns_validated = 2

manager = SummaryManager(
    global_config,
    operation,
    aggregation_table,
    Logger(),
    ["pxcommitdatetime"],
)

summary = manager.build_summary(
    stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)

payload = manager.to_dict(summary)
validation = payload["validation"]

assert validation["validation_count"] == 2
assert validation["passed_validation_count"] == 2
assert validation["failed_validation_count"] == 0
assert validation["disabled_validation_count"] == 0
assert validation["not_run_validation_count"] == 0

print("CHANGE-6E-C-1 PASSED METRICS TEST PASSED")

# ----------------------------------------------------------
# TEST 2
# One validation FAILED
# ----------------------------------------------------------

stats = StatisticsCollector()

stats.timestamp_chunks_validated = 1
stats.timestamp_mismatch_count = 5

stats.rowcount_lob_chunks_validated = 1

summary = manager.build_summary(
    stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)

payload = manager.to_dict(summary)
validation = payload["validation"]

assert validation["validation_count"] == 2
assert validation["passed_validation_count"] == 1
assert validation["failed_validation_count"] == 1
assert validation["disabled_validation_count"] == 0
assert validation["not_run_validation_count"] == 0

print("CHANGE-6E-C-1 FAILED METRICS TEST PASSED")

# ----------------------------------------------------------
# TEST 3
# Both validations DISABLED
# ----------------------------------------------------------

disabled_table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    timestamp_update_validation=False,
    rowcount_lob_validation=False,
)

disabled_manager = SummaryManager(
    global_config,
    operation,
    disabled_table,
    Logger(),
    [],
)

stats = StatisticsCollector()

summary = disabled_manager.build_summary(
    stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)

payload = disabled_manager.to_dict(summary)
validation = payload["validation"]

assert validation["validation_count"] == 0
assert validation["passed_validation_count"] == 0
assert validation["failed_validation_count"] == 0
assert validation["disabled_validation_count"] == 2
assert validation["not_run_validation_count"] == 0

print("CHANGE-6E-C-1 DISABLED METRICS TEST PASSED")

# ----------------------------------------------------------
# TEST 4
# Both validations enabled but NOT_RUN
# ----------------------------------------------------------

stats = StatisticsCollector()

summary = manager.build_summary(
    stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)

payload = manager.to_dict(summary)
validation = payload["validation"]

assert validation["validation_count"] == 2
assert validation["passed_validation_count"] == 0
assert validation["failed_validation_count"] == 0
assert validation["disabled_validation_count"] == 0
assert validation["not_run_validation_count"] == 2

print("CHANGE-6E-C-1 NOT-RUN METRICS TEST PASSED")

print("ALL CHANGE-6E-C-1 TESTS PASSED")

aggregation_manager = SummaryManager(
    global_config,
    operation,
    aggregation_table,
    Logger(),
    ["pxcommitdatetime"],
)
aggregation_summary = aggregation_manager.build_summary(
    aggregation_stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)
aggregation_payload = aggregation_manager.to_dict(aggregation_summary)
validation_payload = aggregation_payload["validation"]

assert validation_payload["overall_status"] == "PASSED"
assert validation_payload["rows_validated"] == 200
assert validation_payload["mismatch_count"] == 0
assert validation_payload["columns_validated"] == 3
assert validation_payload["duration_seconds"] == 5
assert validation_payload["timestamp_update_validation"]["enabled"] is True
assert validation_payload["timestamp_update_validation"]["status"] == "PASSED"
assert validation_payload["timestamp_update_validation"]["rows_validated"] == 100
assert validation_payload["rowcount_lob_validation"]["enabled"] is True
assert validation_payload["rowcount_lob_validation"]["status"] == "PASSED"
assert validation_payload["rowcount_lob_validation"]["rows_validated"] == 100
assert validation_payload["rowcount_lob_validation"]["lob_columns_validated"] == 2

print("CHANGE-6E-B SUMMARY SERIALIZATION TEST PASSED")

print("ALL CHANGE-6D, CHANGE-6E-A AND CHANGE-6E-B TESTS PASSED")

logger = CaptureLogger()

manager = SummaryManager(
    global_config,
    operation,
    aggregation_table,
    logger,
    ["pxcommitdatetime"],
)

manager.write_summary(summary)

assert any("ValidationSummary" in msg for msg in logger.messages)

print("CHANGE-6E-C-2 VALIDATION LOGGING TEST PASSED")

# ------------------------------------------------------------------
# CHANGE-6E-C-3: validation_log_details
# ------------------------------------------------------------------

# TEST 1
# validation_log_details = True
# Detailed validation logging expected

detail_table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    timestamp_update_validation=True,
    rowcount_lob_validation=True,
    validation_log_details=True,
)

detail_stats = StatisticsCollector()

detail_stats.timestamp_chunks_validated = 1
detail_stats.timestamp_rows_validated = 100
detail_stats.timestamp_columns_validated = 1
detail_stats.timestamp_validation_duration_seconds = 2

detail_stats.rowcount_lob_chunks_validated = 1
detail_stats.rowcount_lob_rows_validated = 100
detail_stats.lob_columns_validated = 2
detail_stats.rowcount_lob_validation_duration_seconds = 3

detail_logger = CaptureLogger()

detail_manager = SummaryManager(
    global_config,
    operation,
    detail_table,
    detail_logger,
    ["pxcommitdatetime"],
)

detail_summary = detail_manager.build_summary(
    detail_stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)

detail_manager.write_summary(detail_summary)

assert any("ValidationSummary" in msg for msg in detail_logger.messages)

assert any("TimestampValidationSummary" in msg for msg in detail_logger.messages)

assert any("RowcountLobValidationSummary" in msg for msg in detail_logger.messages)

print("CHANGE-6E-C-3 DETAIL LOGGING TEST PASSED")


# TEST 2
# validation_log_details = False
# Summary-only logging expected

summary_only_table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    timestamp_update_validation=True,
    rowcount_lob_validation=True,
    validation_log_details=False,
)

summary_only_stats = StatisticsCollector()

summary_only_stats.timestamp_chunks_validated = 1
summary_only_stats.timestamp_rows_validated = 100

summary_only_stats.rowcount_lob_chunks_validated = 1
summary_only_stats.rowcount_lob_rows_validated = 100

summary_only_logger = CaptureLogger()

summary_only_manager = SummaryManager(
    global_config,
    operation,
    summary_only_table,
    summary_only_logger,
    ["pxcommitdatetime"],
)

summary_only_summary = summary_only_manager.build_summary(
    summary_only_stats,
    1,
    datetime(2025, 1, 1),
    datetime(2025, 1, 1, 0, 0, 5),
)

summary_only_manager.write_summary(summary_only_summary)

assert any("ValidationSummary" in msg for msg in summary_only_logger.messages)

assert not any(
    "TimestampValidationSummary" in msg for msg in summary_only_logger.messages
)

assert not any(
    "RowcountLobValidationSummary" in msg for msg in summary_only_logger.messages
)

print("CHANGE-6E-C-3 SUMMARY-ONLY LOGGING TEST PASSED")

print("ALL CHANGE-6E-C-3 TESTS PASSED")
