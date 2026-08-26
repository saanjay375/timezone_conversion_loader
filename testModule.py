import json
import os
import tempfile
import sys
import types

from datetime import datetime

from chunking import Chunk

# processor.py imports metadata.py for runtime database processing.
# This isolated unit test exercises CheckpointManager only, so provide
# a minimal metadata stub rather than opening a database connection.
metadata_stub = types.ModuleType("metadata")
metadata_stub.MetadataRepository = object
metadata_stub.get_connection = None
sys.modules.setdefault("metadata", metadata_stub)

from processor import CheckpointManager


def make_range_chunk(chunk_number, start_value, end_value):

    return Chunk(
        chunk_number=chunk_number,
        predicate=(
            f"\"pxcommitdatetime\" >= '{start_value}' "
            f"AND \"pxcommitdatetime\" < '{end_value}'"
        ),
        start_value=datetime.fromisoformat(start_value),
        end_value=datetime.fromisoformat(end_value),
        total_chunks=6,
        is_null_chunk=False,
    )


def make_null_chunk():

    return Chunk(
        chunk_number=6,
        predicate='"pxcommitdatetime" IS NULL',
        start_value=None,
        end_value=None,
        total_chunks=6,
        is_null_chunk=True,
    )


def print_checkpoint(title, checkpoint_manager):

    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    print(json.dumps(checkpoint_manager.load(), indent=4))


with tempfile.TemporaryDirectory() as temp_directory:

    checkpoint_file = os.path.join(
        temp_directory,
        "resume_watermark_checkpoint.json",
    )

    checkpoint_manager = CheckpointManager(checkpoint_file)

    chunks = {
        1: make_range_chunk(
            1,
            "2025-01-01 00:00:00",
            "2025-02-01 00:00:00",
        ),
        2: make_range_chunk(
            2,
            "2025-02-01 00:00:00",
            "2025-03-01 00:00:00",
        ),
        3: make_range_chunk(
            3,
            "2025-03-01 00:00:00",
            "2025-04-01 00:00:00",
        ),
        4: make_range_chunk(
            4,
            "2025-04-01 00:00:00",
            "2025-05-01 00:00:00",
        ),
        5: make_range_chunk(
            5,
            "2025-05-01 00:00:00",
            "2025-06-01 00:00:00",
        ),
    }

    checkpoint_manager.update(chunks[1], 1_000_000)
    checkpoint_manager.update(chunks[2], 1_000_000)
    checkpoint_manager.update(chunks[3], 1_000_000)
    checkpoint_manager.update(chunks[5], 1_500_000)
    checkpoint_manager.update(make_null_chunk(), 500_000)

    checkpoint_before_gap_closes = checkpoint_manager.load()

    assert checkpoint_before_gap_closes["completed_chunks"] == [1, 2, 3, 5]
    assert checkpoint_before_gap_closes["resume_watermark_chunk"] == 3
    assert (
        checkpoint_before_gap_closes["resume_watermark_end_value"]
        == "2025-04-01 00:00:00.000000"
    )
    assert checkpoint_before_gap_closes["null_chunk_completed"] is True

    print_checkpoint(
        "CHECKPOINT BEFORE CHUNK 4 COMPLETES",
        checkpoint_manager,
    )

    checkpoint_manager.update(chunks[4], 45_000_000)

    checkpoint_after_gap_closes = checkpoint_manager.load()

    assert checkpoint_after_gap_closes["completed_chunks"] == [1, 2, 3, 4, 5]
    assert checkpoint_after_gap_closes["resume_watermark_chunk"] == 5
    assert (
        checkpoint_after_gap_closes["resume_watermark_end_value"]
        == "2025-06-01 00:00:00.000000"
    )
    assert checkpoint_after_gap_closes["total_rows_inserted"] == 50_000_000

    print_checkpoint(
        "CHECKPOINT AFTER CHUNK 4 COMPLETES",
        checkpoint_manager,
    )

    print()
    print("RESUME WATERMARK TEST PASSED")
