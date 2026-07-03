"""
worker_pool.py

Thread pool execution framework.

Responsibilities
----------------
- Worker thread lifecycle
- Chunk queue consumption
- Parallel chunk execution
- Statistics aggregation
- Failure tracking
- Pool result reporting

Design Rules
------------
✓ Connection-per-worker (via ChunkProcessor)
✓ Thread-safe queue consumption
✓ ThreadPoolExecutor based
✓ No database code in this module
✓ No SQL generation in this module

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import queue

from dataclasses import dataclass

from concurrent.futures import ThreadPoolExecutor, as_completed

###############################################################################
# WORKER SUMMARY
###############################################################################


@dataclass
class WorkerSummary:

    worker_id: int

    completed_chunks: int = 0

    failed_chunks: int = 0

    rows_loaded: int = 0


###############################################################################
# POOL RESULT
###############################################################################


@dataclass
class WorkerPoolResult:

    completed_chunks: int

    failed_chunks: int

    rows_loaded: int


###############################################################################
# CHUNK WORKER
###############################################################################


class ChunkWorker:

    def __init__(self, worker_id, chunk_queue, chunk_processor, logger):

        self.worker_id = worker_id

        self.chunk_queue = chunk_queue

        self.chunk_processor = chunk_processor

        self.logger = logger

    ###########################################################################
    # MAIN WORKER LOOP
    ###########################################################################

    def run(self):

        summary = WorkerSummary(worker_id=self.worker_id)

        self.logger.info(f"Worker=" f"{self.worker_id} " f"Status=STARTED")

        while True:

            ###################################################################
            # NEXT CHUNK
            ###################################################################

            try:

                chunk = self.chunk_queue.get_nowait()

            except queue.Empty:

                break

            try:

                if chunk.is_null_chunk:

                    self.logger.info(
                        f"Worker={self.worker_id} "
                        f"Chunk={chunk.chunk_number} "
                        f"Range=[NULL_ROWS] "
                        f"Status=ASSIGNED"
                    )

                else:

                    progress_pct = (chunk.chunk_number / chunk.total_chunks) * 100

                    self.logger.info(
                        f"Worker={self.worker_id} "
                        f"Chunk={chunk.chunk_number}/{chunk.total_chunks} "
                        f"({progress_pct:.1f}%) "
                        f"Range=[{chunk.start_value} <= "
                        f"{self.chunk_processor.table_config.driving_column} < "
                        f"{chunk.end_value}] "
                        f"Status=ASSIGNED"
                    )

                result = self.chunk_processor.process_chunk(chunk)

                ################################################################
                # SUCCESS
                ################################################################

                if result.success:

                    summary.completed_chunks += 1

                    summary.rows_loaded += result.rows_inserted

                ################################################################
                # FAILURE
                ################################################################

                else:

                    summary.failed_chunks += 1

            finally:

                self.chunk_queue.task_done()

        #######################################################################
        # WORKER COMPLETE
        #######################################################################

        self.logger.info(
            f"Worker="
            f"{self.worker_id} "
            f"CompletedChunks="
            f"{summary.completed_chunks} "
            f"FailedChunks="
            f"{summary.failed_chunks} "
            f"RowsLoaded="
            f"{summary.rows_loaded}"
        )

        return summary


###############################################################################
# WORKER POOL MANAGER
###############################################################################


class WorkerPoolManager:

    def __init__(self, table_config, chunk_queue, chunk_processor, logger):

        self.table_config = table_config

        self.chunk_queue = chunk_queue

        self.chunk_processor = chunk_processor

        self.logger = logger

    ###########################################################################
    # CREATE WORKERS
    ###########################################################################

    def _build_workers(self):

        workers = []

        for worker_id in range(1, self.table_config.parallel_threads + 1):

            workers.append(
                ChunkWorker(
                    worker_id=worker_id,
                    chunk_queue=self.chunk_queue,
                    chunk_processor=self.chunk_processor,
                    logger=self.logger,
                )
            )

        return workers

    ###########################################################################
    # AGGREGATE
    ###########################################################################

    def _aggregate(self, worker_summaries):

        completed = 0

        failed = 0

        rows_loaded = 0

        for summary in worker_summaries:

            completed += summary.completed_chunks

            failed += summary.failed_chunks

            rows_loaded += summary.rows_loaded

        return WorkerPoolResult(
            completed_chunks=completed, failed_chunks=failed, rows_loaded=rows_loaded
        )

    ###########################################################################
    # EXECUTE
    ###########################################################################

    def execute(self) -> WorkerPoolResult:

        worker_count = self.table_config.parallel_threads

        self.logger.info(f"WorkerPool " f"Workers=" f"{worker_count}")

        workers = self._build_workers()

        worker_summaries = []

        with ThreadPoolExecutor(max_workers=worker_count) as executor:

            futures = [executor.submit(worker.run) for worker in workers]

            for future in as_completed(futures):

                worker_summaries.append(future.result())

        result = self._aggregate(worker_summaries)

        self.logger.info(
            f"WorkerPoolComplete "
            f"Completed="
            f"{result.completed_chunks} "
            f"Failed="
            f"{result.failed_chunks} "
            f"RowsLoaded="
            f"{result.rows_loaded}"
        )

        return result
