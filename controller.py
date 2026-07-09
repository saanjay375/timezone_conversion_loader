"""
controller.py

Orchestration layer.

Responsibilities
----------------
- Logger creation
- DryRun execution
- Table processing
- Worker pool orchestration
- Rowcount validation
- ANALYZE execution
- Summary creation
- Migration controller

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import logging
import os
import json

from dataclasses import dataclass
from datetime import datetime

from metadata import MetadataRepository, get_connection

from chunking import generate_chunks, convert_startvalue, build_chunk_queue

from sql_generator import SQLGenerator

from paths import get_checkpoint_file
from paths import get_execution_log_file
from config import (
    get_effective_postgresql_session_settings,
)

from processor import (
    StatisticsCollector,
    CheckpointManager,
    ChunkProcessor,
    build_table_context,
)
import 
from worker_pool import WorkerPoolManager

from summary import SummaryManager

###############################################################################
# DRYRUN RESULT
###############################################################################


@dataclass
class DryRunResult:

    table_name: str

    target_table: str

    min_value: str | None

    max_value: str | None

    start_value: str | None

    chunk_size: str

    parallel_threads: int

    estimated_chunks: int

    timestamp_columns: list

    table_size_gb: float

    index_present: bool

    validation_status: str


###############################################################################
# LOGGER FACTORY
###############################################################################


class LoggerFactory:

    @staticmethod
    def build_logger(logger_name: str, logfile: str):

        logger = logging.getLogger(logger_name)

        logger.setLevel(logging.INFO)

        if logger.handlers:
            logger.handlers.clear()

        os.makedirs(os.path.dirname(logfile), exist_ok=True)

        formatter = logging.Formatter("%(asctime)s " "%(levelname)s " "%(message)s")

        file_handler = logging.FileHandler(logfile)

        file_handler.setFormatter(formatter)

        console_handler = logging.StreamHandler()

        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)

        logger.addHandler(console_handler)

        return logger


###############################################################################
# DRYRUN ENGINE
###############################################################################


class DryRunEngine:

    def __init__(
        self,
        global_config,
        operation_config,
        table_config,
        logger,
    ):

        self.global_config = global_config
        self.operation_config = operation_config
        self.table_config = table_config
        self.logger = logger

    def execute(self):

        with get_connection(
            self.global_config,
            self.operation_config,
        ) as conn:

            repo = MetadataRepository(conn)

            repo.validate_source_table(
                self.table_config.schema, self.table_config.table_name
            )

            repo.validate_not_partitioned(
                self.table_config.schema, self.table_config.table_name
            )

            repo.validate_driving_column(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            min_value = repo.get_min_value(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            max_value = repo.get_max_value(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            if min_value is None:

                return DryRunResult(
                    table_name=self.table_config.table_name,
                    target_table=(
                        self.table_config.table_name
                        + self.operation_config.target_table_suffix
                    ),
                    estimated_chunks=0,
                    timestamp_columns=[],
                    table_size_gb=0,
                    index_present=False,
                    validation_status="EMPTY_TABLE",
                )

            start_value = (
                convert_startvalue(self.table_config.startvalue)
                if self.table_config.startvalue
                else min_value
            )

            chunks = generate_chunks(
                start_value,
                max_value,
                self.table_config.chunk_size,
                self.table_config.driving_column,
            )
            self.logger.info(f"MinValue={min_value}")

            self.logger.info(f"MaxValue={max_value}")

            self.logger.info(f"StartValue={start_value}")

            self.logger.info(f"ChunkSize={self.table_config.chunk_size}")

            self.logger.info(
                f"ParallelThreads=" f"{self.table_config.parallel_threads}"
            )

            self.logger.info(f"EstimatedChunks={len(chunks)}")

            index_exists, _ = repo.driving_column_index_exists(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            return DryRunResult(
                table_name=self.table_config.table_name,
                target_table=self.table_config.table_name
                + self.operation_config.target_table_suffix,
                min_value=(
                    min_value.strftime("%Y-%m-%d %H:%M:%S") if min_value else None
                ),
                max_value=(
                    max_value.strftime("%Y-%m-%d %H:%M:%S") if max_value else None
                ),
                start_value=(
                    start_value.strftime("%Y-%m-%d %H:%M:%S") if start_value else None
                ),
                chunk_size=self.table_config.chunk_size,
                parallel_threads=self.table_config.parallel_threads,
                estimated_chunks=len(chunks),
                timestamp_columns=repo.get_timestamp_columns(
                    self.table_config.schema, self.table_config.table_name
                ),
                table_size_gb=repo.get_table_size_gb(
                    self.table_config.schema, self.table_config.table_name
                ),
                index_present=index_exists,
                validation_status="VALIDATED",
            )


###############################################################################
# TABLE PROCESSOR
###############################################################################


class TableProcessor:

    def __init__(
        self,
        global_config,
        operation_config,
        table_config,
        logger,
    ):

        self.global_config = global_config
        self.operation_config = operation_config
        self.table_config = table_config
        self.logger = logger

    def execute(self):

        start_time = datetime.now()
        self.logger.info("=" * 80)

        self.logger.info(
            f"TimezoneUpdateStartTime=" f"{start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        )

        self.logger.info(
            f"SourceTable="
            f"{self.table_config.schema}."
            f"{self.table_config.table_name}"
        )

        self.logger.info(
            f"TargetTable="
            f"{self.table_config.table_name}"
            f"{self.operation_config.target_table_suffix}"
        )

        self.logger.info(f"ChunkSize=" f"{self.table_config.chunk_size}")

        self.logger.info(f"ParallelThreads=" f"{self.table_config.parallel_threads}")

        effective_settings = get_effective_postgresql_session_settings(
            self.global_config,
            self.operation_config,
        )

        if effective_settings:

            self.logger.info(
                "PostgreSQLSessionSettings=%s",
                json.dumps(
                    effective_settings,
                    sort_keys=True,
                ),
            )

        self.logger.info("=" * 80)

        with get_connection(
            self.global_config,
            self.operation_config,
        ) as conn:

            repo = MetadataRepository(conn)

            repo.validate_source_table(
                self.table_config.schema, self.table_config.table_name
            )

            repo.validate_not_partitioned(
                self.table_config.schema, self.table_config.table_name
            )

            repo.validate_driving_column(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            target_table = (
                self.table_config.table_name + self.operation_config.target_table_suffix
            )

            repo.create_target_table_if_needed(
                self.table_config.schema, self.table_config.table_name, target_table
            )

            min_value = repo.get_min_value(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            max_value = repo.get_max_value(
                self.table_config.schema,
                self.table_config.table_name,
                self.table_config.driving_column,
            )

            ###################################################################
            # EMPTY TABLE
            ###################################################################

            if min_value is None:

                stats = StatisticsCollector()

                SummaryManager(
                    self.global_config,
                    self.operation_config,
                    self.table_config,
                    self.logger,
                    [],
                ).create_summary(
                    statistics=stats,
                    total_chunks=0,
                    start_time=start_time,
                    end_time=datetime.now(),
                )

                return

            start_value = (
                convert_startvalue(self.table_config.startvalue)
                if self.table_config.startvalue
                else min_value
            )

            chunks = generate_chunks(
                start_value,
                max_value,
                self.table_config.chunk_size,
                self.table_config.driving_column,
            )

            table_context = build_table_context(
                repo, self.table_config.schema, self.table_config.table_name
            )
            timestamp_columns = table_context["timestamp_columns"]

        #######################################################################
        # PROCESSING OBJECTS
        #######################################################################

        statistics = StatisticsCollector()

        checkpoint = CheckpointManager(
            str(
                get_checkpoint_file(
                    self.table_config.schema,
                    self.table_config.table_name,
                )
            )
        )

        sql_generator = SQLGenerator(
            self.global_config, self.operation_config, self.table_config, table_context
        )

        chunk_processor = ChunkProcessor(
            self.global_config,
            self.operation_config,
            self.table_config,
            self.logger,
            sql_generator,
            checkpoint,
            statistics,
        )

        chunk_queue = build_chunk_queue(chunks)

        worker_pool = WorkerPoolManager(
            self.table_config, chunk_queue, chunk_processor, self.logger
        )

        pool_result = worker_pool.execute()

        self.logger.info(
            f"CompletedChunks="
            f"{pool_result.completed_chunks} "
            f"FailedChunks="
            f"{pool_result.failed_chunks} "
            f"RowsLoaded="
            f"{pool_result.rows_loaded}"
        )

        #######################################################################
        # OPTIONAL VALIDATIONS
        #######################################################################

        rowcount_validation = None

        if self.table_config.validate_rowcount:

            with get_connection(
                self.global_config,
                self.operation_config,
            ) as conn:

                repo = MetadataRepository(conn)

                source_count = repo.table_rowcount(
                    self.table_config.schema, self.table_config.table_name
                )

                target_count = repo.table_rowcount(
                    self.table_config.schema, target_table
                )

                rowcount_validation = {
                    "enabled": True,
                    "source_count": source_count,
                    "target_count": target_count,
                    "status": ("MATCH" if source_count == target_count else "MISMATCH"),
                }

        analyze_status = None

        if self.table_config.analyze_after_load:

            with get_connection(
                self.global_config,
                self.operation_config,
            ) as conn:

                repo = MetadataRepository(conn)

                repo.analyze_table(self.table_config.schema, target_table)

                analyze_status = {"enabled": True, "status": "COMPLETED"}

        #######################################################################
        # SUMMARY
        #######################################################################

        SummaryManager(
            self.global_config,
            self.operation_config,
            self.table_config,
            self.logger,
            timestamp_columns,
        ).create_summary(
            statistics=statistics,
            total_chunks=len(chunks),
            start_time=start_time,
            end_time=datetime.now(),
            rowcount_validation=rowcount_validation,
            analyze_status=analyze_status,
        )


###############################################################################
# MIGRATION CONTROLLER
###############################################################################


class MigrationController:

    def __init__(self, global_config):

        self.global_config = global_config

    def execute(self, dryrun=False):

        for operation_cfg in self.global_config.operations:

            for table_cfg in operation_cfg.tables:

                logfile = str(
                    get_execution_log_file(
                        table_cfg.schema,
                        table_cfg.table_name,
                    )
                )

                logger = LoggerFactory.build_logger(
                    f"{table_cfg.schema}." f"{table_cfg.table_name}",
                    logfile,
                )

                if dryrun:

                    result = DryRunEngine(
                        self.global_config,
                        operation_cfg,
                        table_cfg,
                        logger,
                    ).execute()

                    logger.info(f"DryRunResult={result}")

                else:

                    TableProcessor(
                        self.global_config,
                        operation_cfg,
                        table_cfg,
                        logger,
                    ).execute()
