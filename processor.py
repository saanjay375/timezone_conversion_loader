"""
processor.py

Checkpoint manager, statistics collector and chunk processor.

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


class TimestampValidationError(Exception):
    """Raised when timestamp update validation finds mismatched rows."""


@dataclass
class TimestampValidationResult:
    enabled: bool
    rows_validated: int = 0
    mismatch_count: int = 0
    columns_validated: int = 0
    duration_seconds: int = 0
    status: str = "DISABLED"


@dataclass
class RowcountLobValidationResult:
    enabled: bool
    source_count: int = 0
    target_count: int = 0
    mismatch_count: int = 0
    lob_columns_validated: int = 0
    lob_metrics: dict | None = None
    duration_seconds: int = 0
    status: str = "DISABLED"


@dataclass
class ChunkResult:
    chunk_number: int
    rows_inserted: int
    duration_seconds: int
    success: bool
    error_message: str | None = None
    timestamp_validation: TimestampValidationResult | None = None
    rowcount_lob_validation: RowcountLobValidationResult | None = None


class StatisticsCollector:

    def __init__(self):
        self._lock = threading.Lock()
        self.completed_chunks = 0
        self.failed_chunks = 0
        self.total_rows_loaded = 0
        self.null_chunk_processed = False
        self.timestamp_chunks_validated = 0
        self.timestamp_rows_validated = 0
        self.timestamp_mismatch_count = 0
        self.timestamp_columns_validated = 0
        self.timestamp_validation_duration_seconds = 0
        self.rowcount_lob_chunks_validated = 0
        self.rowcount_lob_rows_validated = 0
        self.rowcount_lob_mismatch_count = 0
        self.lob_columns_validated = 0
        self.rowcount_lob_validation_duration_seconds = 0

    def record_success(self, chunk, result: ChunkResult):
        with self._lock:
            self.completed_chunks += 1
            self.total_rows_loaded += result.rows_inserted
            if chunk.is_null_chunk:
                self.null_chunk_processed = True

            validation = result.timestamp_validation
            if validation and validation.enabled:
                self.timestamp_chunks_validated += 1
                self.timestamp_rows_validated += validation.rows_validated
                self.timestamp_mismatch_count += validation.mismatch_count
                self.timestamp_columns_validated = max(
                    self.timestamp_columns_validated,
                    validation.columns_validated,
                )
                self.timestamp_validation_duration_seconds += (
                    validation.duration_seconds
                )

            rowcount_lob = result.rowcount_lob_validation
            if rowcount_lob and rowcount_lob.enabled:
                self.rowcount_lob_chunks_validated += 1
                self.rowcount_lob_rows_validated += rowcount_lob.source_count
                self.rowcount_lob_mismatch_count += rowcount_lob.mismatch_count
                self.lob_columns_validated = max(
                    self.lob_columns_validated,
                    rowcount_lob.lob_columns_validated,
                )
                self.rowcount_lob_validation_duration_seconds += (
                    rowcount_lob.duration_seconds
                )

    def record_failure(self):
        with self._lock:
            self.failed_chunks += 1

    def snapshot(self):
        with self._lock:
            return {
                "completed_chunks": self.completed_chunks,
                "failed_chunks": self.failed_chunks,
                "total_rows_loaded": self.total_rows_loaded,
                "null_chunk_processed": self.null_chunk_processed,
                "timestamp_chunks_validated": self.timestamp_chunks_validated,
                "timestamp_rows_validated": self.timestamp_rows_validated,
                "timestamp_mismatch_count": self.timestamp_mismatch_count,
                "timestamp_columns_validated": self.timestamp_columns_validated,
                "timestamp_validation_duration_seconds": (
                    self.timestamp_validation_duration_seconds
                ),
                "rowcount_lob_chunks_validated": self.rowcount_lob_chunks_validated,
                "rowcount_lob_rows_validated": self.rowcount_lob_rows_validated,
                "rowcount_lob_mismatch_count": self.rowcount_lob_mismatch_count,
                "lob_columns_validated": self.lob_columns_validated,
                "rowcount_lob_validation_duration_seconds": (
                    self.rowcount_lob_validation_duration_seconds
                ),
            }


class CheckpointManager:

    CHECKPOINT_VERSION = 2

    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self._lock = threading.Lock()

    @staticmethod
    def _format_datetime(value):
        if value is None:
            return None
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")

    def _new_state(self):
        return {
            "version": self.CHECKPOINT_VERSION,
            "resume_watermark_chunk": 0,
            "resume_watermark_end_value": None,
            "resume_watermark_predicate": None,
            "completed_chunks": [],
            "completed_chunk_details": {},
            "null_chunk_completed": False,
            "null_chunk_rows_inserted": 0,
            "null_chunk_validation": None,
            "total_rows_inserted": 0,
            "last_update_time": None,
        }

    def _load_unlocked(self):
        if not os.path.exists(self.checkpoint_file):
            return self._new_state()
        with open(self.checkpoint_file, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if payload.get("version") != self.CHECKPOINT_VERSION:
            return self._new_state()
        return payload

    @staticmethod
    def _calculate_resume_watermark(completed_chunks):
        completed_set = {int(value) for value in completed_chunks}
        watermark_chunk = 0
        while watermark_chunk + 1 in completed_set:
            watermark_chunk += 1
        return watermark_chunk

    def _write_unlocked(self, payload):
        tmp_file = self.checkpoint_file + ".tmp"
        checkpoint_directory = os.path.dirname(self.checkpoint_file)
        if checkpoint_directory:
            os.makedirs(checkpoint_directory, exist_ok=True)
        with open(tmp_file, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=4)
        os.replace(tmp_file, self.checkpoint_file)

    @staticmethod
    def _validation_to_dict(validation):
        if validation is None:
            return None
        return dict(validation.__dict__)

    def update(
        self,
        chunk,
        rows_inserted,
        timestamp_validation=None,
        rowcount_lob_validation=None,
    ):
        with self._lock:
            payload = self._load_unlocked()
            validation_payload = self._validation_to_dict(timestamp_validation)
            rowcount_lob_payload = self._validation_to_dict(
                rowcount_lob_validation
            )

            if chunk.is_null_chunk:
                payload["null_chunk_completed"] = True
                payload["null_chunk_rows_inserted"] = (
                    int(payload.get("null_chunk_rows_inserted", 0))
                    + int(rows_inserted)
                )
                payload["null_chunk_validation"] = validation_payload
                payload["null_chunk_rowcount_lob_validation"] = (
                    rowcount_lob_payload
                )
            else:
                chunk_number = int(chunk.chunk_number)
                completed_chunks = {
                    int(value) for value in payload.get("completed_chunks", [])
                }
                completed_chunks.add(chunk_number)
                payload["completed_chunks"] = sorted(completed_chunks)
                details = payload.setdefault("completed_chunk_details", {})
                existing = details.get(str(chunk_number), {})
                cumulative_rows = (
                    int(existing.get("rows_inserted", 0)) + int(rows_inserted)
                )
                details[str(chunk_number)] = {
                    "start_value": self._format_datetime(chunk.start_value),
                    "end_value": self._format_datetime(chunk.end_value),
                    "predicate": chunk.predicate,
                    "rows_inserted": cumulative_rows,
                    "timestamp_validation": validation_payload,
                    "rowcount_lob_validation": rowcount_lob_payload,
                }
                watermark = self._calculate_resume_watermark(
                    payload["completed_chunks"]
                )
                payload["resume_watermark_chunk"] = watermark
                if watermark > 0:
                    watermark_details = details[str(watermark)]
                    payload["resume_watermark_end_value"] = watermark_details[
                        "end_value"
                    ]
                    payload["resume_watermark_predicate"] = watermark_details[
                        "predicate"
                    ]
                else:
                    payload["resume_watermark_end_value"] = None
                    payload["resume_watermark_predicate"] = None

            range_rows = sum(
                int(item.get("rows_inserted", 0))
                for item in payload.get("completed_chunk_details", {}).values()
            )
            payload["total_rows_inserted"] = (
                range_rows + int(payload.get("null_chunk_rows_inserted", 0))
            )
            payload["last_update_time"] = datetime.now().strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            self._write_unlocked(payload)

    def load(self):
        with self._lock:
            if not os.path.exists(self.checkpoint_file):
                return None
            return self._load_unlocked()

    def get_pending_chunks(self, chunks):
        checkpoint = self.load()
        if not checkpoint:
            return list(chunks)
        watermark = int(checkpoint.get("resume_watermark_chunk", 0))
        null_completed = bool(checkpoint.get("null_chunk_completed", False))
        pending = []
        for chunk in chunks:
            if chunk.is_null_chunk:
                if not null_completed:
                    pending.append(chunk)
            elif int(chunk.chunk_number) > watermark:
                pending.append(chunk)
        return pending


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

    def _validate_timestamp_chunk(self, conn, chunk):
        validation_start = time.time()

        with conn.cursor() as cur:
            if chunk.is_null_chunk:
                sql_text = self.sql_generator.build_null_timestamp_validation_sql()
                cur.execute(sql_text)
            else:
                sql_text = self.sql_generator.build_range_timestamp_validation_sql()
                cur.execute(sql_text, (chunk.start_value, chunk.end_value))

            rows_validated, mismatch_count = cur.fetchone()

        result = TimestampValidationResult(
            enabled=True,
            rows_validated=int(rows_validated),
            mismatch_count=int(mismatch_count),
            columns_validated=len(
                self.sql_generator.table_context["timestamp_columns"]
            ),
            duration_seconds=int(time.time() - validation_start),
            status="PASSED" if int(mismatch_count) == 0 else "FAILED",
        )

        self.logger.info(
            f"Chunk={chunk.chunk_number} "
            f"TimestampValidationRows={result.rows_validated} "
            f"TimestampValidationMismatches={result.mismatch_count} "
            f"TimestampValidationStatus={result.status}"
        )

        if result.mismatch_count > 0:
            raise TimestampValidationError(
                f"Timestamp validation failed for chunk {chunk.chunk_number}: "
                f"{result.mismatch_count} mismatched rows"
            )

        return result

    def _validate_rowcount_lob_chunk(self, conn, chunk):
        validation_start = time.time()
        lob_columns = self.sql_generator.table_context.get("lob_columns", [])

        with conn.cursor() as cur:
            if chunk.is_null_chunk:
                sql_text = (
                    self.sql_generator.build_null_rowcount_lob_validation_sql()
                )
                cur.execute(sql_text)
            else:
                sql_text = (
                    self.sql_generator.build_range_rowcount_lob_validation_sql()
                )
                cur.execute(sql_text, (chunk.start_value, chunk.end_value))

            row = cur.fetchone()

        source_count = int(row[0])
        target_count = int(row[1])
        mismatch_count = 0 if source_count == target_count else 1
        lob_metrics = {}
        offset = 2

        for column in lob_columns:
            source_max, source_sum, target_max, target_sum = row[offset:offset + 4]
            source_max = int(source_max or 0)
            source_sum = int(source_sum or 0)
            target_max = int(target_max or 0)
            target_sum = int(target_sum or 0)
            matched = source_max == target_max and source_sum == target_sum
            if not matched:
                mismatch_count += 1
            lob_metrics[column] = {
                "source_max_length": source_max,
                "source_sum_length": source_sum,
                "target_max_length": target_max,
                "target_sum_length": target_sum,
                "status": "MATCH" if matched else "MISMATCH",
            }
            offset += 4

        result = RowcountLobValidationResult(
            enabled=True,
            source_count=source_count,
            target_count=target_count,
            mismatch_count=mismatch_count,
            lob_columns_validated=len(lob_columns),
            lob_metrics=lob_metrics,
            duration_seconds=int(time.time() - validation_start),
            status="PASSED" if mismatch_count == 0 else "FAILED",
        )

        self.logger.info(
            f"Chunk={chunk.chunk_number} "
            f"RowcountLobSourceCount={source_count} "
            f"RowcountLobTargetCount={target_count} "
            f"RowcountLobMismatches={mismatch_count} "
            f"RowcountLobStatus={result.status}"
        )

        if mismatch_count > 0:
            raise ValueError(
                f"Rowcount/LOB validation failed for chunk "
                f"{chunk.chunk_number}: {mismatch_count} mismatch(es)"
            )

        return result

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
                    if chunk.is_null_chunk:
                        sql_text = self.sql_generator.build_null_chunk_sql()
                        cur.execute(sql_text)
                    else:
                        sql_text = self.sql_generator.build_range_insert_sql()
                        cur.execute(sql_text, (chunk.start_value, chunk.end_value))

                    rows_inserted = cur.rowcount
                    conn.commit()

                timestamp_validation = None

                if self.table_config.timestamp_update_validation:
                    timestamp_validation = self._validate_timestamp_chunk(conn, chunk)

                rowcount_lob_validation = None

                if self.table_config.rowcount_lob_validation:
                    rowcount_lob_validation = self._validate_rowcount_lob_chunk(
                        conn, chunk
                    )

                self.checkpoint_manager.update(
                    chunk,
                    rows_inserted,
                    timestamp_validation,
                    rowcount_lob_validation,
                )

                duration = int(time.time() - start_time)
                result = ChunkResult(
                    chunk_number=chunk.chunk_number,
                    rows_inserted=rows_inserted,
                    duration_seconds=duration,
                    success=True,
                    timestamp_validation=timestamp_validation,
                    rowcount_lob_validation=rowcount_lob_validation,
                )

                self.statistics.record_success(chunk, result)

                if chunk.is_null_chunk:
                    self.logger.info(
                        f"Chunk={chunk.chunk_number} Range=[NULL_ROWS] "
                        f"RowsInserted={rows_inserted} Duration={duration}s "
                        f"Status=COMPLETED"
                    )
                else:
                    progress_pct = (chunk.chunk_number / chunk.total_chunks) * 100
                    self.logger.info(
                        f"Chunk={chunk.chunk_number}/{chunk.total_chunks} "
                        f"({progress_pct:.1f}%) "
                        f"Range=[{chunk.start_value} <= "
                        f"{self.table_config.driving_column} < {chunk.end_value}] "
                        f"RowsInserted={rows_inserted} Duration={duration}s "
                        f"Status=COMPLETED"
                    )

                return result

        except Exception as ex:
            duration = int(time.time() - start_time)
            self.statistics.record_failure()
            self.logger.error(
                f"Chunk={chunk.chunk_number} Duration={duration}s "
                f"Status=FAILED Error={str(ex)}"
            )
            return ChunkResult(
                chunk_number=chunk.chunk_number,
                rows_inserted=0,
                duration_seconds=duration,
                success=False,
                error_message=str(ex),
            )


def build_table_context(metadata_repository, schema, table_name):
    all_columns = metadata_repository.get_all_columns(schema, table_name)
    timestamp_columns = metadata_repository.get_timestamp_columns(schema, table_name)
    primary_key_columns = metadata_repository.get_primary_key_columns(
        schema,
        table_name,
    )
    lob_columns = metadata_repository.get_lob_columns(schema, table_name)
    return {
        "all_columns": all_columns,
        "timestamp_columns": timestamp_columns,
        "primary_key_columns": primary_key_columns,
        "lob_columns": lob_columns,
    }
