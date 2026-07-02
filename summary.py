"""
summary.py

Summary generation utilities.

Responsibilities
----------------
- Final status calculation
- Summary object creation
- Summary JSON file creation
- Atomic summary file writing
- Migration statistics reporting

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import json
import os

from enum import Enum
from dataclasses import dataclass

from datetime import datetime


###############################################################################
# STATUS ENUM
###############################################################################

class SummaryStatus(Enum):

    COMPLETED = "COMPLETED"

    COMPLETED_WITH_ERRORS = (
        "COMPLETED_WITH_ERRORS"
    )

    FAILED = "FAILED"


###############################################################################
# SUMMARY MODEL
###############################################################################

@dataclass
class SummaryFile:

    schema: str

    source_table: str

    target_table: str

    source_timezone: str

    target_timezone: str

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


###############################################################################
# SUMMARY MANAGER
###############################################################################

class SummaryManager:

    def __init__(
        self,
        global_config,
        table_config,
        logger
    ):

        self.global_config = global_config

        self.table_config = table_config

        self.logger = logger

    ###########################################################################
    # FILE PATH
    ###########################################################################

    def get_summary_file_path(
        self
    ):

        return os.path.join(

            self.global_config.log_directory,

            f"{self.table_config.schema}."

            f"{self.table_config.table_name}"

            ".summary.json"
        )

    ###########################################################################
    # STATUS CALCULATION
    ###########################################################################

    def determine_status(
        self,
        statistics,
        execution_failed=False
    ):

        if execution_failed:

            return SummaryStatus.FAILED

        if statistics.failed_chunks > 0:

            return (
                SummaryStatus.
                COMPLETED_WITH_ERRORS
            )

        return SummaryStatus.COMPLETED

    ###########################################################################
    # BUILD SUMMARY OBJECT
    ###########################################################################

    def build_summary(
        self,
        statistics,
        total_chunks,
        start_time,
        end_time,
        rowcount_validation=None,
        analyze_status=None,
        execution_failed=False
    ):

        duration_seconds = int(

            (
                end_time
                -
                start_time
            ).total_seconds()
        )

        target_table = (

            self.table_config.table_name

            +

            self.global_config.
            newtablenamesuffix
        )

        return SummaryFile(

            schema=
                self.table_config.schema,

            source_table=
                self.table_config.table_name,

            target_table=
                target_table,

            source_timezone=
                self.global_config.
                source_timezone,

            target_timezone=
                self.global_config.
                target_timezone,

            status=
                self.determine_status(
                    statistics,
                    execution_failed
                ).value,

            driving_column=
                self.table_config.
                driving_column,

            chunk_size=
                self.table_config.
                chunk_size,

            parallel_threads=
                self.table_config.
                parallel_threads,

            startvalue=
                self.table_config.
                startvalue,

            total_chunks=
                total_chunks,

            completed_chunks=
                statistics.
                completed_chunks,

            failed_chunks=
                statistics.
                failed_chunks,

            null_chunk_processed=
                statistics.
                null_chunk_processed,

            total_rows_loaded=
                statistics.
                total_rows_loaded,

            rowcount_validation=
                rowcount_validation,

            analyze_status=
                analyze_status,

            start_time=
                start_time,

            end_time=
                end_time,

            duration_seconds=
                duration_seconds
        )

    ###########################################################################
    # CONVERT TO JSON PAYLOAD
    ###########################################################################

    def to_dict(
        self,
        summary: SummaryFile
    ):

        return {

            ###################################################################
            # TABLE INFO
            ###################################################################

            "schema":
                summary.schema,

            "source_table":
                summary.source_table,

            "target_table":
                summary.target_table,

            ###################################################################
            # TIMEZONE INFO
            ###################################################################

            "source_timezone":
                summary.source_timezone,

            "target_timezone":
                summary.target_timezone,

            ###################################################################
            # STATUS
            ###################################################################

            "status":
                summary.status,

            ###################################################################
            # CONFIGURATION
            ###################################################################

            "driving_column":
                summary.driving_column,

            "chunk_size":
                summary.chunk_size,

            "parallel_threads":
                summary.parallel_threads,

            "startvalue":
                summary.startvalue,

            ###################################################################
            # CHUNK STATISTICS
            ###################################################################

            "total_chunks":
                summary.total_chunks,

            "completed_chunks":
                summary.completed_chunks,

            "failed_chunks":
                summary.failed_chunks,

            "null_chunk_processed":
                summary.null_chunk_processed,

            ###################################################################
            # LOAD STATISTICS
            ###################################################################

            "total_rows_loaded":
                summary.total_rows_loaded,

            ###################################################################
            # VALIDATION
            ###################################################################

            "rowcount_validation":
                summary.rowcount_validation,

            ###################################################################
            # ANALYZE
            ###################################################################

            "analyze_status":
                summary.analyze_status,

            ###################################################################
            # TIMING
            ###################################################################

            "start_time":
                summary.start_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

            "end_time":
                summary.end_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),

            "duration_seconds":
                summary.duration_seconds
        }

    ###########################################################################
    # WRITE FILE
    ###########################################################################

    def write_summary(
        self,
        summary: SummaryFile
    ):

        payload = self.to_dict(
            summary
        )

        summary_file = (
            self.get_summary_file_path()
        )

        tmp_file = (
            summary_file
            +
            ".tmp"
        )

        os.makedirs(
            os.path.dirname(
                summary_file
            ),
            exist_ok=True
        )

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
            summary_file
        )

        self.logger.info(

            f"SummaryFileCreated="
            f"{summary_file}"
        )

    ###########################################################################
    # BUILD + WRITE
    ###########################################################################

    def create_summary(
        self,
        statistics,
        total_chunks,
        start_time,
        end_time,
        rowcount_validation=None,
        analyze_status=None,
        execution_failed=False
    ):

        summary = self.build_summary(

            statistics=
                statistics,

            total_chunks=
                total_chunks,

            start_time=
                start_time,

            end_time=
                end_time,

            rowcount_validation=
                rowcount_validation,

            analyze_status=
                analyze_status,

            execution_failed=
                execution_failed
        )

        self.write_summary(
            summary
        )

        return summary