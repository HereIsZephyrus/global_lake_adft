"""Worker-side resident-slice execution for MPI batch computation."""

from __future__ import annotations

import logging
import time

from .protocol import TAG_STATUS, TAG_DATA, WorkerSliceAssignment, WorkerState, _iter_id_batches

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        rank: int,
        algorithm: str,
        calculator,
        chunk_size: int,
    ) -> None:
        self._rank = rank
        self._algorithm = algorithm
        self._calculator = calculator
        self._chunk_size = chunk_size

    def _send(self, comm, state: str, stats: dict) -> None:
        comm.send((state, stats), dest=0, tag=TAG_STATUS)

    def run_dataset_id_batch(
        self,
        comm,
        factory,
        assignment: WorkerSliceAssignment | None,
    ) -> None:
        overall_started = time.perf_counter()
        if assignment is None or not assignment.id_subset:
            self._send(comm, WorkerState.DONE, {
                "source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0, "elapsed_seconds": 0.0,
            })
            return

        total_stats = {"source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0, "elapsed_seconds": 0.0}
        slice_started = time.perf_counter()
        resident_slice = factory.preload(assignment.id_subset)
        read_elapsed = time.perf_counter() - slice_started
        slice_dataset = resident_slice.dataset
        query_batches = _iter_id_batches(slice_dataset.hylak_ids.tolist(), assignment.chunk_size)

        self._send(comm, WorkerState.PRELOADING, {
            "source": len(slice_dataset),
            "read_seconds": read_elapsed,
        })
        log.info(
            "Worker %d: preloaded resident slice lakes=%d queries=%d read=%.3fs",
            self._rank,
            len(slice_dataset),
            len(query_batches),
            read_elapsed,
        )

        total_stats["source"] = len(slice_dataset)
        self._send(comm, WorkerState.CALCULATING, {
            "source": len(slice_dataset),
            "chunks": len(query_batches),
        })

        for i, id_batch in enumerate(query_batches):
            calc_started = time.perf_counter()
            dataset = resident_slice.build_query(frozenset(int(hid) for hid in id_batch))
            rows_by_table, success_lakes, error_lakes = self._calculator.run_dataset(
                dataset,
                error_chunk=(0, 1),
            )
            calc_elapsed = time.perf_counter() - calc_started

            if any(rows_by_table.values()):
                comm.send(dict(rows_by_table), dest=0, tag=TAG_DATA)

            total_stats["success"] += success_lakes
            total_stats["error"] += error_lakes
            total_stats["chunks"] += 1
            total_stats["elapsed_seconds"] = time.perf_counter() - overall_started

            log.info(
                "Worker %d: finished query %d/%d success=%d error=%d calc=%.3fs total=%.3fs",
                self._rank,
                i + 1,
                len(query_batches),
                success_lakes,
                error_lakes,
                calc_elapsed,
                total_stats["elapsed_seconds"],
            )

        self._send(comm, WorkerState.DONE, total_stats)
