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

from config import DatabaseConfig, GlobalConfig, OperationConfig, TableConfig
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
    def __init__(self, validation_row): self.validation_row = validation_row
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql_text, values=None): pass
    def fetchone(self): return self.validation_row

class Connection:
    def __init__(self, validation_row): self.validation_row = validation_row
    def cursor(self): return Cursor(self.validation_row)
    def commit(self): pass

class Repository:
    def __init__(self, connection): pass
    def configure_session(self, statement_timeout_ms, lock_timeout_ms): pass

class Logger:
    def info(self, message, *args): print(message % args if args else message)
    def error(self, message, *args): print(message % args if args else message)

class SQL:
    table_context = {
        "timestamp_columns": [],
        "lob_columns": ["message_text", "payload_bytes"],
    }
    def build_range_insert_sql(self): return "INSERT"
    def build_null_chunk_sql(self): return "INSERT NULL"
    def build_range_rowcount_lob_validation_sql(self): return "VALIDATE ROWCOUNT LOB"
    def build_null_rowcount_lob_validation_sql(self): return "VALIDATE NULL ROWCOUNT LOB"

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
    100, 100,
    250, 10000, 250, 10000,
    4096, 250000, 4096, 250000,
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
        stats, 1, datetime(2025, 1, 1), datetime(2025, 1, 1, 0, 0, 5)
    )
    payload = manager.to_dict(summary)
    validation = payload["validation"]["rowcount_lob_validation"]
    assert validation["status"] == "PASSED"
    assert validation["rows_validated"] == 100
    assert validation["lob_columns_validated"] == 2

print("CHANGE-6D PASS PATH TEST PASSED")

FAILING_ROW = (
    100, 99,
    250, 10000, 250, 9999,
    4096, 250000, 4096, 250000,
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
        "all_columns": ["id", "pxcommitdatetime", "message_text", "payload_bytes"],
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
