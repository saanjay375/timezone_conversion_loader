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
    def __init__(self, result): self.result = result
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def execute(self, sql_text, values=None): pass
    def fetchone(self): return self.result

class Connection:
    def __init__(self, result): self.result = result
    def cursor(self): return Cursor(self.result)
    def commit(self): pass

class Repository:
    def __init__(self, connection): pass
    def configure_session(self, statement_timeout_ms, lock_timeout_ms): pass

class Logger:
    def info(self, message, *args): print(message % args if args else message)
    def error(self, message, *args): print(message % args if args else message)

class SQL:
    table_context = {"timestamp_columns": ["created_at", "updated_at"]}
    def build_range_insert_sql(self): return "INSERT"
    def build_null_chunk_sql(self): return "INSERT NULL"
    def build_range_timestamp_validation_sql(self): return "VALIDATE"
    def build_null_timestamp_validation_sql(self): return "VALIDATE NULL"

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
    timestamp_update_validation=True,
)

import processor
processor.MetadataRepository = Repository

@contextmanager
def passing_connection(*args, **kwargs):
    yield Connection((100, 0))

with tempfile.TemporaryDirectory() as temp:
    processor.get_connection = passing_connection
    checkpoint = CheckpointManager(os.path.join(temp, "checkpoint.json"))
    stats = StatisticsCollector()
    worker = ChunkProcessor(
        global_config, operation, table, Logger(), SQL(), checkpoint, stats
    )
    result = worker.process_chunk(Chunk())
    assert result.success
    assert result.timestamp_validation.status == "PASSED"
    assert checkpoint.load()["resume_watermark_chunk"] == 1

    manager = SummaryManager(
        global_config, operation, table, Logger(), ["created_at", "updated_at"]
    )
    summary = manager.build_summary(
        stats, 1, datetime(2025, 1, 1), datetime(2025, 1, 1, 0, 0, 5)
    )
    payload = manager.to_dict(summary)
    assert payload["validation"]["timestamp_update_validation"]["status"] == "PASSED"
    assert payload["validation"]["timestamp_update_validation"]["rows_validated"] == 100

print("CHANGE-6C PASS PATH TEST PASSED")

@contextmanager
def failing_connection(*args, **kwargs):
    yield Connection((100, 3))

with tempfile.TemporaryDirectory() as temp:
    processor.get_connection = failing_connection
    checkpoint = CheckpointManager(os.path.join(temp, "checkpoint.json"))
    stats = StatisticsCollector()
    worker = ChunkProcessor(
        global_config, operation, table, Logger(), SQL(), checkpoint, stats
    )
    result = worker.process_chunk(Chunk())
    assert not result.success
    assert stats.failed_chunks == 1
    assert checkpoint.load() is None

print("CHANGE-6C FAILURE BLOCKS WATERMARK TEST PASSED")

sql = SQLGenerator(
    global_config,
    operation,
    table,
    {
        "all_columns": ["id", "pxcommitdatetime", "created_at"],
        "timestamp_columns": ["pxcommitdatetime", "created_at"],
        "primary_key_columns": ["id"],
    },
).build_range_timestamp_validation_sql()
assert "IS DISTINCT FROM" in sql
assert "LEFT JOIN" in sql
assert 's."pxcommitdatetime" >= %s' in sql
print("CHANGE-6C SQL GENERATION TEST PASSED")
