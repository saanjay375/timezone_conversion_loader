from contextlib import contextmanager
from datetime import datetime
from time import perf_counter, sleep

import controller
from config import DatabaseConfig, GlobalConfig, OperationConfig, TableConfig
from controller import DryRunEngine


class TestLogger:

    def info(self, message, *args):
        if args:
            message = message % args
        print(message)


class ValidationRepository:

    def __init__(self, connection):
        self.connection = connection

    def validate_source_table(self, schema, table_name):
        return None

    def validate_not_partitioned(self, schema, table_name):
        return None

    def validate_driving_column(self, schema, table_name, driving_column):
        return None


@contextmanager
def fake_get_connection(global_config, operation_config=None):
    yield object()


class ParallelDryRunEngine(DryRunEngine):

    TASK_DELAY_SECONDS = 0.25

    def _task(self, value):
        sleep(self.TASK_DELAY_SECONDS)
        return value

    def _get_min_value(self):
        return self._task(datetime(2025, 1, 1, 0, 0, 0))

    def _get_max_value(self):
        return self._task(datetime(2025, 6, 30, 12, 0, 0))

    def _get_timestamp_columns(self):
        return self._task([
            "pxcommitdatetime",
            "pxcreatedatetime",
        ])

    def _get_table_size_gb(self):
        return self._task(10.5)

    def _get_index_status(self):
        return self._task(True)


controller.get_connection = fake_get_connection
controller.MetadataRepository = ValidationRepository


database = DatabaseConfig(
    host="localhost",
    port=5432,
    dbname="test_database",
    username="test_user",
    password="test_password",
)

global_config = GlobalConfig(
    database=database,
)

operation_config = OperationConfig(
    type="timezone_update",
    source_timezone="America/New_York",
    target_timezone="UTC",
    target_table_suffix="_utc",
)

table_config = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    parallel_threads=4,
)

engine = ParallelDryRunEngine(
    global_config,
    operation_config,
    table_config,
    TestLogger(),
)

start_time = perf_counter()
result = engine.execute()
elapsed_seconds = perf_counter() - start_time

print()
print("=" * 80)
print("PARALLEL DRYRUN TEST RESULT")
print("=" * 80)
print(result)
print(f"ElapsedSeconds={elapsed_seconds:.3f}")

assert result.table_name == "pr_index_test"
assert result.target_table == "pr_index_test_utc"
assert result.min_value == "2025-01-01 00:00:00"
assert result.max_value == "2025-06-30 12:00:00"
assert result.timestamp_columns == [
    "pxcommitdatetime",
    "pxcreatedatetime",
]
assert result.table_size_gb == 10.5
assert result.index_present is True
assert result.validation_status == "VALIDATED"

# Five tasks each sleep for 0.25 seconds. Serial execution would take at least
# 1.25 seconds. Parallel execution should complete comfortably below 0.75.
assert elapsed_seconds < 0.75

print()
print("PARALLEL DRYRUN TEST PASSED")
