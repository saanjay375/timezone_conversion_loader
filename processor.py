"""
processor.py

Checkpoint manager
Statistics collector
Chunk processor

Design Goals
------------
✓ Atomic checkpoint updates
✓ Thread-safe statistics
✓ INSERT ... ON CONFLICT DO NOTHING
✓ No per-chunk COUNT(*)
✓ RowsInserted only
✓ NULL chunk support
✓ Transaction rollback on failure
✓ Connection-per-worker model

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import json
import os
import threading
import time

from dataclasses import dataclass
from datetime import datetime

from metadata import MetadataRepository, get_connection

###############################################################################
# CHUNK RESULT
###############################################################################


@dataclass
class ChunkResult:

    chunk_number: int

    rows_inserted: int

    duration_seconds: int

    success: bool

    error_message: str | None = None


###############################################################################
# STATISTICS COLLECTOR
###############################################################################


class StatisticsCollector:

    def __init__(self):

        self._lock = threading.Lock()

        self.completed_chunks = 0

        self.failed_chunks = 0

        self.total_rows_loaded = 0

        self.null_chunk_processed = False

    ###########################################################################
    # SUCCESS
    ###########################################################################

    def record_success(self, chunk, result: ChunkResult):

        with self._lock:

            self.completed_chunks += 1

            self.total_rows_loaded += result.rows_inserted

            if chunk.is_null_chunk:

                self.null_chunk_processed = True

    ###########################################################################
    # FAILURE
    ###########################################################################

    def record_failure(self):

        with self._lock:

            self.failed_chunks += 1

    ###########################################################################
    # SNAPSHOT
    ###########################################################################

    def snapshot(self):

        with self._lock:

            return {
                "completed_chunks": self.completed_chunks,
                "failed_chunks": self.failed_chunks,
                "total_rows_loaded": self.total_rows_loaded,
                "null_chunk_processed": self.null_chunk_processed,
            }


###############################################################################
# CHECKPOINT MANAGER
###############################################################################


class CheckpointManager:

    def __init__(self, checkpoint_file: str):

        self.checkpoint_file = checkpoint_file

        self._lock = threading.Lock()

    ###########################################################################
    # UPDATE
    ###########################################################################

    def update(self, chunk, rows_inserted):

        payload = {
            "last_completed_chunk": chunk.chunk_number,
            "last_successful_predicate": chunk.predicate,
            "rows_inserted": rows_inserted,
            "last_update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        tmp_file = self.checkpoint_file + ".tmp"

        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)

        with self._lock:

            with open(tmp_file, "w", encoding="utf-8") as handle:

                json.dump(payload, handle, indent=4)

            os.replace(tmp_file, self.checkpoint_file)

    ###########################################################################
    # LOAD
    ###########################################################################

    def load(self):

        if not os.path.exists(self.checkpoint_file):
            return None

        with open(self.checkpoint_file, "r", encoding="utf-8") as handle:

            return json.load(handle)


###############################################################################
# CHUNK PROCESSOR
###############################################################################


class ChunkProcessor:

    def __init__(
        self,
        global_config,
        operation_config,
        table_config,
        logger,
        sql_generator,
        checkpoint,
        statistics,
    ):

        self.global_config = global_config
        self.operation_config = operation_config
        self.table_config = table_config
        self.logger = logger
        self.sql_generator = sql_generator

        self.checkpoint_manager = checkpoint

        self.statistics = statistics

    ###########################################################################
    # PROCESS SINGLE CHUNK
    ###########################################################################

    def process_chunk(self, chunk) -> ChunkResult:

        start_time = time.time()

        try:

            with get_connection(
                self.global_config,
                self.operation_config,
            ) as conn:

                metadata = MetadataRepository(conn)

                metadata.configure_session(
                    self.global_config.statement_timeout_ms,
                    self.global_config.lock_timeout_ms,
                )

                with conn.cursor() as cur:

                    ###################################################################
                    # SQL
                    ###################################################################

                    if chunk.is_null_chunk:

                        sql_text = self.sql_generator.build_null_chunk_sql()

                        cur.execute(sql_text)

                    else:

                        sql_text = self.sql_generator.build_range_insert_sql()

                        cur.execute(sql_text, (chunk.start_value, chunk.end_value))

                    ###################################################################
                    # ROWCOUNT
                    ###################################################################

                    rows_inserted = cur.rowcount

                    ###################################################################
                    # COMMIT
                    ###################################################################

                    conn.commit()

                #######################################################################
                # CHECKPOINT
                #######################################################################

                self.checkpoint_manager.update(chunk, rows_inserted)

                duration = int(time.time() - start_time)

                result = ChunkResult(
                    chunk_number=chunk.chunk_number,
                    rows_inserted=rows_inserted,
                    duration_seconds=duration,
                    success=True,
                )

                self.statistics.record_success(chunk, result)

                if chunk.is_null_chunk:

                    self.logger.info(
                        f"Chunk={chunk.chunk_number} "
                        f"Range=[NULL_ROWS] "
                        f"RowsInserted={rows_inserted} "
                        f"Duration={duration}s "
                        f"Status=COMPLETED"
                    )

                else:

                    progress_pct = (chunk.chunk_number / chunk.total_chunks) * 100

                    self.logger.info(
                        f"Chunk={chunk.chunk_number}/{chunk.total_chunks} "
                        f"({progress_pct:.1f}%) "
                        f"Range=[{chunk.start_value} <= "
                        f"{self.table_config.driving_column} < "
                        f"{chunk.end_value}] "
                        f"RowsInserted={rows_inserted} "
                        f"Duration={duration}s "
                        f"Status=COMPLETED"
                    )

                return result

        except Exception as ex:

            duration = int(time.time() - start_time)

            self.statistics.record_failure()

            self.logger.error(
                f"Chunk="
                f"{chunk.chunk_number} "
                f"Duration="
                f"{duration}s "
                f"Status=FAILED "
                f"Error="
                f"{str(ex)}"
            )

            return ChunkResult(
                chunk_number=chunk.chunk_number,
                rows_inserted=0,
                duration_seconds=duration,
                success=False,
                error_message=str(ex),
            )


###############################################################################
# TABLE CONTEXT BUILDER
###############################################################################


def build_table_context(metadata_repository, schema, table_name):

    all_columns = metadata_repository.get_all_columns(schema, table_name)

    timestamp_columns = metadata_repository.get_timestamp_columns(schema, table_name)

    return {"all_columns": all_columns, "timestamp_columns": timestamp_columns}
