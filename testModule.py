from config import (
    TableConfig,
)

table = TableConfig(
    schema="repack",
    table_name="pr_index_test",
    driving_column="pxcommitdatetime",
    chunk_size="1M",
    parallel_threads=4,
    timestamp_update_validation=True,
    rowcount_lob_validation=True,
)

print()
print("=" * 80)
print("VALIDATION CONFIG TEST")
print("=" * 80)

print(table)

assert table.timestamp_update_validation is True

assert table.rowcount_lob_validation is True

print()
print("VALIDATION CONFIG TEST PASSED")
