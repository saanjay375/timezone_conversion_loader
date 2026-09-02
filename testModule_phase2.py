import os
import sys
import tempfile
import types

from datetime import datetime

from chunking import Chunk

metadata_stub = types.ModuleType("metadata")
metadata_stub.MetadataRepository = object
metadata_stub.get_connection = None
sys.modules.setdefault("metadata", metadata_stub)

from processor import CheckpointManager


def range_chunk(number, month):
    start = datetime(2025, month, 1)
    end = datetime(2025, month + 1, 1)
    return Chunk(
        chunk_number=number,
        predicate=f"chunk-{number}",
        start_value=start,
        end_value=end,
        total_chunks=6,
        is_null_chunk=False,
    )


def null_chunk():
    return Chunk(
        chunk_number=6,
        predicate="NULL_ROWS",
        start_value=None,
        end_value=None,
        total_chunks=6,
        is_null_chunk=True,
    )


with tempfile.TemporaryDirectory() as temp_dir:
    path = os.path.join(temp_dir, "checkpoint.json")
    manager = CheckpointManager(path)
    chunks = [range_chunk(i, i) for i in range(1, 6)] + [null_chunk()]

    for chunk_number in (1, 2, 3, 5):
        manager.update(chunks[chunk_number - 1], 1)
    manager.update(chunks[-1], 0)

    pending = manager.get_pending_chunks(chunks)
    pending_numbers = [chunk.chunk_number for chunk in pending]

    print("Checkpoint:", manager.load())
    print("Pending chunk numbers:", pending_numbers)

    assert manager.load()["resume_watermark_chunk"] == 3
    assert pending_numbers == [4, 5]

    manager.update(chunks[3], 1)
    manager.update(chunks[4], 0)

    pending_after_resume = manager.get_pending_chunks(chunks)
    pending_after_resume_numbers = [
        chunk.chunk_number for chunk in pending_after_resume
    ]

    print("Final checkpoint:", manager.load())
    print("Pending after resume:", pending_after_resume_numbers)

    assert manager.load()["resume_watermark_chunk"] == 5
    assert pending_after_resume_numbers == []

    print("RESUME WATERMARK PHASE-2 TEST PASSED")
