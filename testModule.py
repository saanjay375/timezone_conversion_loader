from config import (
    DatabaseConfig,
    GlobalConfig,
    OperationConfig,
    TableConfig,
)

from processor import (
    CheckpointManager,
    ChunkProcessor,
    StatisticsCollector,
)


class DummyLogger:

    def info(self, message):
        print(f"INFO: {message}")

    def error(self, message):
        print(f"ERROR: {message}")


class DummySQLGenerator:

    pass


db_config = DatabaseConfig(
    host="localhost",
    port=5432,
    dbname="postgres",
    username="postgres",
    password="postgres",
)

global_config = GlobalConfig(
    database=db_config,
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

checkpoint_manager = CheckpointManager("test_checkpoint.json")

statistics = StatisticsCollector()

processor = ChunkProcessor(
    global_config,
    operation_config,
    table_config,
    DummyLogger(),
    DummySQLGenerator(),
    checkpoint_manager,
    statistics,
)

print()
print("=" * 80)
print("CHUNK PROCESSOR ATTRIBUTE TEST")
print("=" * 80)

print("Has checkpoint_manager:")
print(hasattr(processor, "checkpoint_manager"))

print()
print("checkpoint_manager object:")
print(processor.checkpoint_manager)

print()
print("Has operation_config:")
print(hasattr(processor, "operation_config"))

print()
print("operation_config:")
print(processor.operation_config)
