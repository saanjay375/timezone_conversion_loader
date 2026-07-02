"""
processor.py

Checkpoint manager
Statistics collector
Chunk processor

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time

from dataclasses import dataclass
from datetime import datetime

from metadata import (
    MetadataRepository,
    get_connection
)


###############################################################################
# RESULT OBJECT
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

        self.lock = threading.Lock()

        self.completed_chunks = 0

        self.failed_chunks = 0

        self.total_rows_loaded = 0

        self.null_chunk_processed = False

    def record_success(
        self,
        chunk,
        result
    ):

        with self.lock:

            self.completed_chunks += 1

            self.total_rows_loaded += (
                result.rows_inserted
            )

            if chunk.is_null_chunk:

                self.null_chunk_processed = True

    def record_failure(self):

        with self.lock:

            self.failed_chunks += 1


###############################################################################
# CHECKPOINT MANAGER
###############################################################################

class CheckpointManager:

    def __init__(
        self,
        checkpoint_file
    ):

        self.checkpoint_file = (
            checkpoint_file
        )

        self.lock = threading.Lock()

    def update(
        self,
        chunk,
        rows_inserted
    ):

        payload = {

            "last_completed_chunk":
                chunk.chunk_number,

            "rows_inserted":
                rows_inserted,

            "last_successful_predicate":
                chunk.predicate,

            "last_update_time":
                datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
        }

        tmp_file = (
            self.checkpoint_file
            +
            ".tmp"
        )

        with self.lock:

            with open(
                tmp_file,
                "w",
                encoding="utf-8"
            ) as handle:

                json.dump(
                    payload,
                    handle,
                    indent=4
                )

            os.replace(
                tmp_file,
                self.checkpoint_file
            )


###############################################################################
# CHUNK PROCESSOR
###############################################################################

class ChunkProcessor:

    def __init__(
        self,
        global_config,
        table_config,
        logger,
        sql_generator,
        checkpoint_manager,
        statistics
    ):

        self.global_config = (
            global_config
        )

        self.table_config = (
            table_config
        )

        self.logger = logger

        self.sql_generator = (
            sql_generator
        )

        self.checkpoint_manager = (
            checkpoint_manager
        )

        self.statistics = (
            statistics
        )

    ###########################################################################
    # EXECUTE ONE CHUNK
    ###########################################################################

    def process_chunk(
        self,
        chunk
    ):

        start_time = time.time()

        try:

            with get_connection(
                self.global_config
            ) as conn:

                metadata = (
                    MetadataRepository(
                        conn
                    )
                )

                metadata.configure_session(

                    self.global_config.
                    statement_timeout_ms,

                    self.global_config.
                    lock_timeout_ms
                )

                with conn.cursor() as cur:

                    ###########################################################
                    # BUILD SQL
                    ###########################################################

                    if chunk.is_null_chunk:

                        sql_text = (
                            self.sql_generator
                            .build_null_chunk_sql()
                        )

                        bind_values = None

                    else:

                        sql_text = (
                            self.sql_generator
                            .build_range_insert_sql()
                        )

                        bind_values = (

                            chunk.start_value,

                            chunk.end_value
                        )

                    ###########################################################
                    # EXECUTE
                    ###########################################################

                    if bind_values:

                        cur.execute(
                            sql_text,
                            bind_values
                        )

                    else:

                        cur.execute(
                            sql_text
                        )

                    ###########################################################
                    # INSERTED ROWS
                    ###########################################################

                    rows_inserted = (
                        cur.rowcount
                    )

                    ###########################################################
                    # COMMIT
                    ###########################################################

                    conn.commit()

                ###############################################################
                # CHECKPOINT
                ###############################################################

                self.checkpoint_manager.update(

                    chunk,

                    rows_inserted
                )

                ###############################################################
                # RESULT
                ###############################################################

                duration = int(

                    time.time()
                    -
                    start_time
                )

                result = ChunkResult(

                    chunk_number=
                        chunk.chunk_number,

                    rows_inserted=
                        rows_inserted,

                    duration_seconds=
                        duration,

                    success=True
                )

                ###############################################################
                # STATS
                ###############################################################

                self.statistics.record_success(

                    chunk,

                    result
                )

                ###############################################################
                # LOG
                ###############################################################

                self.logger.info(

                    f"Chunk="
                    f"{chunk.chunk_number} "

                    f"RowsInserted="
                    f"{rows_inserted} "

                    f"Duration="
                    f"{duration}s "

                    f"Status=COMPLETED"
                )

                return result

        except Exception as ex:

            duration = int(

                time.time()
                -
                start_time
            )

            self.statistics.record_failure()

            self.logger.error(

                f"Chunk="
                f"{chunk.chunk_number} "

                f"Status=FAILED "

                f"Duration="
                f"{duration}s "

                f"Error={str(ex)}"
            )

            return ChunkResult(

                chunk_number=
                    chunk.chunk_number,

                rows_inserted=0,

                duration_seconds=
                    duration,

                success=False,

                error_message=
                    str(ex)
            )


###############################################################################
# TABLE CONTEXT BUILDER
###############################################################################

def build_table_context(
    metadata_repository,
    schema,
    table_name
):

    all_columns = (
        metadata_repository
        .get_all_columns(
            schema,
            table_name
        )
    )

    timestamp_columns = (
        metadata_repository
        .get_timestamp_columns(
            schema,
            table_name
        )
    )

    return {

        "all_columns":
            all_columns,

        "timestamp_columns":
            timestamp_columns
    }