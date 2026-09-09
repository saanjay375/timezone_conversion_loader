"""
summary.py

Migration summary generation.

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import json
import os

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from config import get_effective_postgresql_session_settings
from paths import get_summary_file


class SummaryStatus(Enum):
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"
    FAILED = "FAILED"


@dataclass
class OperationInfo:
    type: str
    source_timezone: str
    target_timezone: str
    updated_columns: list
    updated_column_count: int
    driving_column: str


@dataclass
class ValidationInfo:
    validation_fail_on_error: bool = False
    validation_log_details: bool = True
    overall_status: str = "DISABLED"
    rows_validated: int = 0
    mismatch_count: int = 0
    columns_validated: int = 0
    duration_seconds: int = 0
    timestamp_update_validation: dict | None = None
    rowcount_lob_validation: dict | None = None
    validation_count: int = 0

    passed_validation_count: int = 0

    failed_validation_count: int = 0

    disabled_validation_count: int = 0

    not_run_validation_count: int = 0


@dataclass
class SummaryFile:
    schema: str
    source_table: str
    target_table: str
    status: str
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
    validation: ValidationInfo | None = None


class SummaryManager:

    def __init__(
        self,
        global_config,
        operation_config,
        table_config,
        logger,
        timestamp_columns=None,
    ):
        self.global_config = global_config
        self.operation_config = operation_config
        self.table_config = table_config
        self.logger = logger
        self.timestamp_columns = timestamp_columns or []

    def get_summary_file_path(self):
        return str(
            get_summary_file(
                self.table_config.schema,
                self.table_config.table_name,
            )
        )

    def build_validation_metadata(self, statistics):
        aggregate = statistics.build_validation_summary(
            self.table_config.timestamp_update_validation,
            self.table_config.rowcount_lob_validation,
        )
        validation_count = aggregate.validation_count

        passed_validation_count = aggregate.passed_validation_count

        failed_validation_count = aggregate.failed_validation_count

        disabled_validation_count = aggregate.disabled_validation_count

        not_run_validation_count = aggregate.not_run_validation_count
        timestamp_validation = {
            "enabled": self.table_config.timestamp_update_validation,
            "status": aggregate.timestamp_update_validation_status,
        }
        if self.table_config.timestamp_update_validation:
            timestamp_validation.update(
                {
                    "chunks_validated": statistics.timestamp_chunks_validated,
                    "rows_validated": statistics.timestamp_rows_validated,
                    "mismatch_count": statistics.timestamp_mismatch_count,
                    "columns_validated": statistics.timestamp_columns_validated,
                    "duration_seconds": (
                        statistics.timestamp_validation_duration_seconds
                    ),
                }
            )

        rowcount_lob_validation = {
            "enabled": self.table_config.rowcount_lob_validation,
            "status": aggregate.rowcount_lob_validation_status,
        }
        if self.table_config.rowcount_lob_validation:
            rowcount_lob_validation.update(
                {
                    "chunks_validated": statistics.rowcount_lob_chunks_validated,
                    "rows_validated": statistics.rowcount_lob_rows_validated,
                    "mismatch_count": statistics.rowcount_lob_mismatch_count,
                    "lob_columns_validated": statistics.lob_columns_validated,
                    "duration_seconds": (
                        statistics.rowcount_lob_validation_duration_seconds
                    ),
                }
            )
        validation_fail_on_error = self.table_config.validation_fail_on_error

        validation_log_details = self.table_config.validation_log_details
        return ValidationInfo(
            validation_fail_on_error=validation_fail_on_error,
            validation_log_details=validation_log_details,
            overall_status=aggregate.overall_status,
            rows_validated=aggregate.rows_validated,
            mismatch_count=aggregate.mismatch_count,
            validation_count=validation_count,
            passed_validation_count=passed_validation_count,
            failed_validation_count=failed_validation_count,
            disabled_validation_count=disabled_validation_count,
            not_run_validation_count=not_run_validation_count,
            columns_validated=(
                aggregate.timestamp_columns_validated + aggregate.lob_columns_validated
            ),
            duration_seconds=aggregate.duration_seconds,
            timestamp_update_validation=timestamp_validation,
            rowcount_lob_validation=rowcount_lob_validation,
        )

    def determine_status(self, statistics, execution_failed=False):

        if execution_failed:
            return SummaryStatus.FAILED

        if self.table_config.validation_fail_on_error and statistics.failed_chunks > 0:
            return SummaryStatus.FAILED

        if statistics.failed_chunks > 0:
            return SummaryStatus.COMPLETED_WITH_ERRORS

        return SummaryStatus.COMPLETED

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
            self.table_config.table_name + self.operation_config.target_table_suffix
        )
        operation = OperationInfo(
            type=self.operation_config.type,
            source_timezone=self.operation_config.source_timezone,
            target_timezone=self.operation_config.target_timezone,
            updated_columns=self.timestamp_columns,
            updated_column_count=len(self.timestamp_columns),
            driving_column=self.table_config.driving_column,
        )

        return SummaryFile(
            schema=self.table_config.schema,
            source_table=self.table_config.table_name,
            target_table=target_table,
            status=self.determine_status(statistics, execution_failed).value,
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
            operation=operation,
            validation=self.build_validation_metadata(statistics),
        )

    @staticmethod
    def operation_to_dict(operation: OperationInfo | None):
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

    @staticmethod
    def validation_to_dict(validation: ValidationInfo | None):
        if validation is None:
            return None
        return {
            "validation_fail_on_error": validation.validation_fail_on_error,
            "validation_log_details": validation.validation_log_details,
            "overall_status": validation.overall_status,
            "rows_validated": validation.rows_validated,
            "mismatch_count": validation.mismatch_count,
            "columns_validated": validation.columns_validated,
            "duration_seconds": validation.duration_seconds,
            "timestamp_update_validation": (validation.timestamp_update_validation),
            "rowcount_lob_validation": validation.rowcount_lob_validation,
            "validation_count": validation.validation_count,
            "passed_validation_count": validation.passed_validation_count,
            "failed_validation_count": validation.failed_validation_count,
            "disabled_validation_count": validation.disabled_validation_count,
            "not_run_validation_count": validation.not_run_validation_count,
        }

    def to_dict(self, summary: SummaryFile):
        return {
            "schema": summary.schema,
            "source_table": summary.source_table,
            "target_table": summary.target_table,
            "operation": self.operation_to_dict(summary.operation),
            "validation": self.validation_to_dict(summary.validation),
            "postgresql_session_settings": (
                get_effective_postgresql_session_settings(
                    self.global_config,
                    self.operation_config,
                )
            ),
            "status": summary.status,
            "chunk_size": summary.chunk_size,
            "parallel_threads": summary.parallel_threads,
            "startvalue": summary.startvalue,
            "total_chunks": summary.total_chunks,
            "completed_chunks": summary.completed_chunks,
            "failed_chunks": summary.failed_chunks,
            "null_chunk_processed": summary.null_chunk_processed,
            "total_rows_loaded": summary.total_rows_loaded,
            "rowcount_validation": summary.rowcount_validation,
            "analyze_status": summary.analyze_status,
            "start_time": summary.start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "end_time": summary.end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "duration_seconds": summary.duration_seconds,
        }

    def write_summary(self, summary: SummaryFile):
        payload = self.to_dict(summary)
        validation = summary.validation

        if validation is not None:

            # Always log the aggregate validation summary
            self.logger.info(
                "ValidationSummary "
                f"OverallStatus={validation.overall_status} "
                f"ValidationCount={validation.validation_count} "
                f"PassedValidations={validation.passed_validation_count} "
                f"FailedValidations={validation.failed_validation_count} "
                f"DisabledValidations={validation.disabled_validation_count} "
                f"NotRunValidations={validation.not_run_validation_count} "
                f"RowsValidated={validation.rows_validated} "
                f"MismatchCount={validation.mismatch_count} "
                f"Duration={validation.duration_seconds}s"
            )

            # Detailed validation logging only when enabled
            if self.table_config.validation_log_details:

                timestamp_validation = validation.timestamp_update_validation

                if timestamp_validation is not None:
                    self.logger.info(
                        "TimestampValidationSummary "
                        f"Enabled={timestamp_validation.get('enabled')} "
                        f"Status={timestamp_validation.get('status')} "
                        f"ChunksValidated={timestamp_validation.get('chunks_validated', 0)} "
                        f"RowsValidated={timestamp_validation.get('rows_validated', 0)} "
                        f"ColumnsValidated={timestamp_validation.get('columns_validated', 0)} "
                        f"MismatchCount={timestamp_validation.get('mismatch_count', 0)} "
                        f"Duration={timestamp_validation.get('duration_seconds', 0)}s"
                    )

                rowcount_lob_validation = validation.rowcount_lob_validation

                if rowcount_lob_validation is not None:
                    self.logger.info(
                        "RowcountLobValidationSummary "
                        f"Enabled={rowcount_lob_validation.get('enabled')} "
                        f"Status={rowcount_lob_validation.get('status')} "
                        f"ChunksValidated={rowcount_lob_validation.get('chunks_validated', 0)} "
                        f"RowsValidated={rowcount_lob_validation.get('rows_validated', 0)} "
                        f"LobColumnsValidated={rowcount_lob_validation.get('lob_columns_validated', 0)} "
                        f"MismatchCount={rowcount_lob_validation.get('mismatch_count', 0)} "
                        f"Duration={rowcount_lob_validation.get('duration_seconds', 0)}s"
                    )

        summary_file = self.get_summary_file_path()
        tmp_file = summary_file + ".tmp"

        summary_directory = os.path.dirname(summary_file)

        if summary_directory:
            os.makedirs(summary_directory, exist_ok=True)

        with open(tmp_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=4)

        os.replace(tmp_file, summary_file)

        self.logger.info(f"SummaryFileCreated={summary_file}")

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
