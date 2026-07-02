"""
worker_pool.py

Chunk worker pool execution layer.

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import queue

from dataclasses import dataclass

from concurrent.futures import (
    ThreadPoolExecutor,
    as_completed,
)


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
# CHUNK WORKER
###############################################################################

class ChunkWorker:

    def __init__(
        self,
        worker_id,
        chunk_queue,
        chunk_processor,
        logger
    ):

        self.worker_id = worker_id

        self.chunk_queue = chunk_queue

        self.chunk_processor = chunk_processor

        self.logger = logger

    ###########################################################################
    # MAIN LOOP
    ###########################################################################

    def run(self):

        summary = WorkerSummary(
            worker_id=self.worker_id
        )

        self.logger.info(

            f"Worker="
            f"{self.worker_id} "
            f"Status=STARTED"
        )

        while True:

            ###################################################################
            # GET NEXT CHUNK
            ###################################################################

            try:

                chunk = (
                    self.chunk_queue
                    .get_nowait()
                )

            except queue.Empty:

                break

            try:

                self.logger.info(

                    f"Worker="
                    f"{self.worker_id} "

                    f"Chunk="
                    f"{chunk.chunk_number} "

                    f"Status=ASSIGNED"
                )

                result = (
                    self.chunk_processor
                    .process_chunk(
                        chunk
                    )
                )

                ###############################################################
                # SUCCESS
                ###############################################################

                if result.success:

                    summary.completed_chunks += 1

                    summary.rows_loaded += (
                        result.rows_inserted
                    )

                ###############################################################
                # FAILURE
                ###############################################################

                else:

                    summary.failed_chunks += 1

            finally:

                self.chunk_queue.task_done()

        #######################################################################
        # FINAL STATISTICS
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
# POOL RESULT
###############################################################################

@dataclass
class WorkerPoolResult:

    completed_chunks: int

    failed_chunks: int

    rows_loaded: int


###############################################################################
# WORKER POOL
###############################################################################

class WorkerPoolManager:

    def __init__(
        self,
        table_config,
        chunk_queue,
        chunk_processor,
        logger
    ):

        self.table_config = table_config

        self.chunk_queue = chunk_queue

        self.chunk_processor = chunk_processor

        self.logger = logger

    ###########################################################################
    # EXECUTE
    ###########################################################################

    def execute(self):

        worker_count = (
            self.table_config
            .parallel_threads
        )

        self.logger.info(

            f"WorkerPool "
            f"Workers="
            f"{worker_count}"
        )

        worker_summaries = []

        with ThreadPoolExecutor(
            max_workers=worker_count
        ) as executor:

            futures = []

            ###################################################################
            # CREATE WORKERS
            ###################################################################

            for worker_id in range(
                1,
                worker_count + 1
            ):

                worker = ChunkWorker(

                    worker_id=
                        worker_id,

                    chunk_queue=
                        self.chunk_queue,

                    chunk_processor=
                        self.chunk_processor,

                    logger=
                        self.logger
                )

                futures.append(

                    executor.submit(
                        worker.run
                    )
                )

            ###################################################################
            # COLLECT RESULTS
            ###################################################################

            for future in as_completed(
                futures
            ):

                worker_summaries.append(
                    future.result()
                )

        #######################################################################
        # AGGREGATE
        #######################################################################

        total_completed = 0

        total_failed = 0

        total_rows_loaded = 0

        for summary in worker_summaries:

            total_completed += (
                summary.completed_chunks
            )

            total_failed += (
                summary.failed_chunks
            )

            total_rows_loaded += (
                summary.rows_loaded
            )

        #######################################################################
        # LOG
        #######################################################################

        self.logger.info(

            f"WorkerPoolComplete "

            f"Completed="
            f"{total_completed} "

            f"Failed="
            f"{total_failed} "

            f"RowsLoaded="
            f"{total_rows_loaded}"
        )

        return WorkerPoolResult(

            completed_chunks=
                total_completed,

            failed_chunks=
                total_failed,

            rows_loaded=
                total_rows_loaded
        )