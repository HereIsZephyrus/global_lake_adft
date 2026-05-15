"""Manager status tracker for MPI batch computation.

Workers send computed data to rank 0 via TAG_DATA; Manager collects and flushes.
"""

from __future__ import annotations

from collections import defaultdict
import logging
import time

from .protocol import RunReport, TAG_STATUS, TAG_DATA, WorkerSliceAssignment, WorkerState, _iter_id_batches

log = logging.getLogger(__name__)


class Manager:
    def __init__(self, comm, size: int, lake_filter=None, writer=None) -> None:
        self._comm = comm
        self._size = size
        self._lake_filter = lake_filter
        self._writer = writer
        self._n_workers = size - 1
        self._worker_states: dict[int, str] = {}
        self._done_workers: set[int] = set()
        self._report = RunReport()
        self._pending_rows: dict[str, list[dict]] = defaultdict(list)
        self._chunks_since_flush = 0
        self._flush_interval = 50
        self._started_at = time.perf_counter()

    @property
    def report(self) -> RunReport:
        return self._report

    def _poll_and_dispatch(self) -> None:
        from mpi4py import MPI

        if self._comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_DATA):
            status = MPI.Status()
            data = self._comm.recv(source=MPI.ANY_SOURCE, tag=TAG_DATA, status=status)
            self._merge_rows(data)

        if not self._comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_STATUS):
            time.sleep(0.01)
            return

        status = MPI.Status()
        msg = self._comm.recv(source=MPI.ANY_SOURCE, tag=TAG_STATUS, status=status)
        worker = status.Get_source()
        state, stats = msg
        self._on_status(worker, state, stats)

    def _on_status(self, worker: int, state: str, stats: dict) -> None:
        self._worker_states[worker] = state

        if state == WorkerState.PRELOADING:
            log.debug(
                "Worker %d preloading slice_lakes=%d read=%.3fs",
                worker,
                stats.get("source", 0),
                stats.get("read_seconds", 0.0),
            )
            return

        if state == WorkerState.CALCULATING:
            log.debug(
                "Worker %d calculating chunks=%d source=%d",
                worker,
                stats.get("chunks", 0),
                stats.get("source", 0),
            )
            return

        if state == WorkerState.DONE:
            self._report.source_lakes += stats.get("source", 0)
            self._report.skipped_lakes += stats.get("skipped", 0)
            self._report.success_lakes += stats.get("success", 0)
            self._report.error_lakes += stats.get("error", 0)
            self._report.processed_chunks += stats.get("chunks", 0)
            self._done_workers.add(worker)
            log.info(
                "Worker %d: done (success=%d error=%d chunks=%d elapsed=%.3fs)",
                worker,
                stats.get("success", 0),
                stats.get("error", 0),
                stats.get("chunks", 0),
                stats.get("elapsed_seconds", 0.0),
            )

    def _drain_data(self) -> None:
        from mpi4py import MPI
        while self._comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_DATA):
            data = self._comm.recv(source=MPI.ANY_SOURCE, tag=TAG_DATA)
            self._merge_rows(data)

    def _merge_rows(self, data: dict[str, list[dict]]) -> None:
        for table_name, rows in data.items():
            self._pending_rows[table_name].extend(rows)
        self._chunks_since_flush += 1
        if self._chunks_since_flush >= self._flush_interval:
            self._flush()

    def _flush(self) -> None:
        if not any(self._pending_rows.values()):
            return
        if self._writer is None:
            log.warning("No writer set, cannot flush data")
            return
        total_rows = sum(len(rows) for rows in self._pending_rows.values())
        self._writer.persist(dict(self._pending_rows))
        log.info("Flushed %d rows across %d tables", total_rows, len(self._pending_rows))
        self._pending_rows = defaultdict(list)
        self._chunks_since_flush = 0

    def run_dataset_id_batch(
        self,
        sorted_ids: list[int],
        batch_size: int,
        algorithm: str,
    ) -> RunReport:
        assignments = self._assign_worker_slices(sorted_ids, batch_size, algorithm)
        self._comm.bcast(assignments, root=0)

        self._report.total_chunks = sum(
            len(_iter_id_batches(sorted(assignment.id_subset), assignment.chunk_size))
            for assignment in assignments.values()
        )

        while len(self._done_workers) < self._n_workers:
            self._poll_and_dispatch()

        self._drain_data()
        self._flush()
        self._comm.bcast(None, root=0)
        log.info(
            "Dataset ID-batch all workers done: success=%d error=%d skipped=%d elapsed=%.3fs",
            self._report.success_lakes,
            self._report.error_lakes,
            self._report.skipped_lakes,
            time.perf_counter() - self._started_at,
        )
        return self._report

    def _assign_worker_slices(
        self,
        sorted_ids: list[int],
        batch_size: int,
        algorithm: str,
    ) -> dict[int, WorkerSliceAssignment]:
        """Split already-filtered IDs into worker-scoped resident slices."""
        base = len(sorted_ids) // self._n_workers if self._n_workers else 0
        remainder = len(sorted_ids) % self._n_workers if self._n_workers else 0

        assignments: dict[int, WorkerSliceAssignment] = {}
        idx = 0
        for r in range(1, self._size):
            count = base + (1 if r <= remainder else 0)
            worker_ids = frozenset(sorted_ids[idx : idx + count])
            assignments[r] = WorkerSliceAssignment(
                id_subset=worker_ids,
                chunk_size=batch_size,
                algorithm=algorithm,
            )
            idx += count

        log.info(
            "Assigned %d workers across %d lakes: %s",
            self._n_workers,
            len(sorted_ids),
            {r: len(a.id_subset) for r, a in assignments.items()},
        )
        return assignments
