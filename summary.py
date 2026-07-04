"""
summary.py

Migration summary generation.

Responsibilities
----------------
- Final status determination
- Summary object creation
- JSON summary file creation
- Atomic file writing
- Migration statistics reporting

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import json
import os

from enum import Enum
from dataclasses import dataclass

from datetime import datetime
from paths import get_summary_file

###############################################################################
# STATUS ENUM
###############################################################################


class SummaryStatus(Enum):

    COMPLETED = "COMPLETED"

    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"

    FAILED = "FAILED"


###############################################################################
# SUMMARY MODEL
###############################################################################


<<<<<<< HEAD
=======
@dataclass
class OperationInfo:

    type: str

    source_timezone: str

    target_timezone: str

    updated_columns: list

    updated_column_count: int

    driving_column: str


>>>>>>> 2036b50 (Updated logging)
@dataclass
class SummaryFile:

    schema: str

    source_table: str

    target_table: str

    status: str

    driving_column: str

    chunk_size: str

    parallel_threads: int

    startvalue: str | None

    total_chunks: int

    completed_chunks: int

    failed_chunks: int

    null_chunk_processed: bool

    total_rows_loaded: int

    rowcount_validation: dict | None

    analyze_status: dict | None

    start_time: datetime

    end_time: datetime

    duration_seconds: int

    operation: OperationInfo | None = None


###############################################################################
# SUMMARY MANAGER
###############################################################################


class SummaryManager:

<<<<<<< HEAD
    def __init__(self, global_config, table_config, logger):
=======
    def __init__(
        self,
        global_config,
        table_config,
        logger,
        timestamp_columns=None,
    ):
>>>>>>> 2036b50 (Updated logging)

        self.global_config = global_config

        self.table_config = table_config

        self.logger = logger

        self.timestamp_columns = timestamp_columns or []

    ###########################################################################
    # FILE PATH
    ###########################################################################

    def get_summary_file_path(self):

        return str(
            get_summary_file(
                self.table_config.schema,
                self.table_config.table_name,
            )
        )

    ###########################################################################
    # STATUS
    ###########################################################################

    def determine_status(self, statistics, execution_failed=False):

        if execution_failed:

            return SummaryStatus.FAILED

        if statistics.failed_chunks > 0:

            return SummaryStatus.COMPLETED_WITH_ERRORS

        return SummaryStatus.COMPLETED

    ###########################################################################
    # BUILD SUMMARY
    ###########################################################################

    def build_summary(
        self,
        statistics,
        total_chunks,
        start_time,
        end_time,
        rowcount_validation=None,
        analyze_status=None,
        execution_failed=False,
    ):

        duration_seconds = int((end_time - start_time).total_seconds())

        target_table = (
            self.table_config.table_name + self.global_config.newtablenamesuffix
<<<<<<< HEAD
=======
        )

        operation = OperationInfo(
            type="timezone_update",
            source_timezone=self.global_config.source_timezone,
            target_timezone=self.global_config.target_timezone,
            updated_columns=self.timestamp_columns,
            updated_column_count=len(self.timestamp_columns),
            driving_column=self.table_config.driving_column,
>>>>>>> 2036b50 (Updated logging)
        )

        return SummaryFile(
            schema=self.table_config.schema,
            source_table=self.table_config.table_name,
            target_table=target_table,
<<<<<<< HEAD
            source_timezone=self.global_config.source_timezone,
            target_timezone=self.global_config.target_timezone,
=======
>>>>>>> 2036b50 (Updated logging)
            status=self.determine_status(statistics, execution_failed).value,
            driving_column=self.table_config.driving_column,
            chunk_size=self.table_config.chunk_size,
            parallel_threads=self.table_config.parallel_threads,
            startvalue=self.table_config.startvalue,
            total_chunks=total_chunks,
            completed_chunks=statistics.completed_chunks,
            failed_chunks=statistics.failed_chunks,
            null_chunk_processed=statistics.null_chunk_processed,
            total_rows_loaded=statistics.total_rows_loaded,
            rowcount_validation=rowcount_validation,
            analyze_status=analyze_status,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=duration_seconds,
<<<<<<< HEAD
=======
            operation=operation,
>>>>>>> 2036b50 (Updated logging)
        )

    ###########################################################################
    # TO DICT
    ###########################################################################
    def operation_to_dict(self, operation: OperationInfo | None):

<<<<<<< HEAD
    def to_dict(self, summary: SummaryFile):

        return {
=======
        if operation is None:

            return None

        return {
            "type": operation.type,
            "source_timezone": operation.source_timezone,
            "target_timezone": operation.target_timezone,
            "updated_columns": operation.updated_columns,
            "updated_column_count": operation.updated_column_count,
            "driving_column": operation.driving_column,
        }

    def to_dict(self, summary: SummaryFile):

        return {
>>>>>>> 2036b50 (Updated logging)
            ###################################################################
            # TABLE
            ###################################################################
            "schema": summary.schema,
            "source_table": summary.source_table,
            "target_table": summary.target_table,
            ###################################################################
            # OPERATION
            ###################################################################
<<<<<<< HEAD
            "source_timezone": summary.source_timezone,
            "target_timezone": summary.target_timezone,
=======
            "operation": self.operation_to_dict(summary.operation),
>>>>>>> 2036b50 (Updated logging)
            ###################################################################
            # STATUS
            ###################################################################
            "status": summary.status,
            ###################################################################
            # CONFIGURATION
            ###################################################################
            "driving_column": summary.driving_column,
            "chunk_size": summary.chunk_size,
            "parallel_threads": summary.parallel_threads,
            "startvalue": summary.startvalue,
            ###################################################################
            # CHUNKS
            ###################################################################
            "total_chunks": summary.total_chunks,
            "completed_chunks": summary.completed_chunks,
            "failed_chunks": summary.failed_chunks,
            "null_chunk_processed": summary.null_chunk_processed,
            ###################################################################
            # LOAD METRICS
            ###################################################################
            "total_rows_loaded": summary.total_rows_loaded,
            ###################################################################
            # VALIDATION
            ###################################################################
            "rowcount_validation": summary.rowcount_validation,
            ###################################################################
            # ANALYZE
            ###################################################################
            "analyze_status": summary.analyze_status,
            ###################################################################
            # TIMING
            ###################################################################
            "start_time": summary.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": summary.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": summary.duration_seconds,
        }

    ###########################################################################
    # WRITE SUMMARY
    ###########################################################################

    def write_summary(self, summary: SummaryFile):

        payload = self.to_dict(summary)

        summary_file = self.get_summary_file_path()

        tmp_file = summary_file + ".tmp"

        os.makedirs(os.path.dirname(summary_file), exist_ok=True)

        with open(tmp_file, "w", encoding="utf-8") as handle:

            json.dump(payload, handle, indent=4)

        os.replace(tmp_file, summary_file)

        self.logger.info(f"SummaryFileCreated=" f"{summary_file}")

    ###########################################################################
    # CREATE SUMMARY
    ###########################################################################

    def create_summary(
        self,
        statistics,
        total_chunks,
        start_time,
        end_time,
        rowcount_validation=None,
        analyze_status=None,
        execution_failed=False,
    ):

        summary = self.build_summary(
            statistics=statistics,
            total_chunks=total_chunks,
            start_time=start_time,
            end_time=end_time,
            rowcount_validation=rowcount_validation,
            analyze_status=analyze_status,
            execution_failed=execution_failed,
        )

        self.write_summary(summary)

        return summary
