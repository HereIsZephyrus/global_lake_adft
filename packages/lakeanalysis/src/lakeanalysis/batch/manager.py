"""Manager: IO scheduler and status tracker for MPI batch computation.

IO slot lifecycle:
  - TRIGGER_READ/TRIGGER_WRITE sent → io_active += 1
  - CALCULATING received (reading ended) → io_active -= 1
  - PENDING received with prev=WRITING (writing ended) → io_active -= 1
"""

from __future__ import annotations

import logging
import time
from collections import deque

from .protocol import RunReport, TAG_STATUS, TAG_TRIGGER, TRIGGER_READ, TRIGGER_WRITE, WorkerState, _iter_chunk_ranges

log = logging.getLogger(__name__)


class Manager:
    def __init__(self, comm, size: int, io_budget: int = 4) -> None:
        self._comm = comm
        self._size = size
        self._io_budget = io_budget
        self._io_active = 0
        self._n_workers = size - 1
        self._worker_states: dict[int, str] = {}
        self._read_queue: deque[int] = deque()
        self._write_queue: deque[int] = deque()
        self._done_workers: set[int] = set()
        self._report = RunReport()

    @property
    def report(self) -> RunReport:
        return self._report

    def run(
        self, max_hylak_id: int, chunk_size: int, limit_id: int | None
    ) -> RunReport:
        assignments = self._assign(max_hylak_id, chunk_size, limit_id)
        self._comm.bcast(assignments, root=0)

        self._report.total_chunks = sum(
            len(_iter_chunk_ranges(e - 1, chunk_size, e))
            for (_, e) in assignments.values()
        )

        while len(self._done_workers) < self._n_workers:
            self._poll_and_dispatch()

        self._comm.bcast(None, root=0)
        log.info(
            "All workers done: success=%d error=%d skipped=%d",
            self._report.success_lakes,
            self._report.error_lakes,
            self._report.skipped_lakes,
        )
        return self._report

    def _poll_and_dispatch(self) -> None:
        from mpi4py import MPI

        if not self._comm.iprobe(source=MPI.ANY_SOURCE, tag=TAG_STATUS):
            time.sleep(0.01)
            return

        status = MPI.Status()
        msg = self._comm.recv(source=MPI.ANY_SOURCE, tag=TAG_STATUS, status=status)
        worker = status.Get_source()
        state, stats = msg
        self._on_status(worker, state, stats)
        self._schedule_io()

    def _on_status(self, worker: int, state: str, stats: dict) -> None:
        prev = self._worker_states.get(worker)
        self._worker_states[worker] = state

        if state == WorkerState.CALCULATING:
            self._io_active -= 1
            self._report.source_lakes += stats.get("source", 0)
            self._report.skipped_lakes += stats.get("skipped", 0)
            self._report.success_lakes += stats.get("success", 0)
            self._report.error_lakes += stats.get("error", 0)
            self._write_queue.append(worker)
            log.debug(
                "Worker %d: calculating (success=%d error=%d) io_active=%d",
                worker,
                stats.get("success", 0),
                stats.get("error", 0),
                self._io_active,
            )

        elif state == WorkerState.PENDING:
            if prev == WorkerState.WRITING:
                self._io_active -= 1
                self._read_queue.append(worker)
                log.debug("Worker %d: pending (after write) io_active=%d", worker, self._io_active)
            elif prev == WorkerState.CALCULATING:
                self._write_queue.append(worker)
                log.debug("Worker %d: pending (after calc)", worker)
            else:
                self._read_queue.append(worker)
                log.debug("Worker %d: pending (initial)", worker)

        elif state == WorkerState.DONE:
            self._report.source_lakes += stats.get("source", 0)
            self._report.skipped_lakes += stats.get("skipped", 0)
            self._report.success_lakes += stats.get("success", 0)
            self._report.error_lakes += stats.get("error", 0)
            self._report.processed_chunks += stats.get("chunks", 0)
            self._done_workers.add(worker)
            log.info(
                "Worker %d: done (success=%d error=%d chunks=%d)",
                worker,
                stats.get("success", 0),
                stats.get("error", 0),
                stats.get("chunks", 0),
            )

    def _schedule_io(self) -> None:
        while self._read_queue and self._io_active < self._io_budget:
            worker = self._read_queue.popleft()
            self._comm.send(TRIGGER_READ, dest=worker, tag=TAG_TRIGGER)
            self._io_active += 1

        while self._write_queue and self._io_active < self._io_budget:
            worker = self._write_queue.popleft()
            self._comm.send(TRIGGER_WRITE, dest=worker, tag=TAG_TRIGGER)
            self._io_active += 1

    def _assign(
        self, max_hylak_id: int, chunk_size: int, limit_id: int | None
    ) -> dict[int, tuple[int, int]]:
        upper = max_hylak_id
        if limit_id is not None:
            upper = min(upper, limit_id - 1)
        if upper < 0:
            return {r: (0, 0) for r in range(1, self._size)}

        per_worker = (upper + self._n_workers) // self._n_workers
        assignments: dict[int, tuple[int, int]] = {}
        for r in range(1, self._size):
            start = (r - 1) * per_worker
            end = min(r * per_worker, upper + 1)
            assignments[r] = (start, end)

        log.info(
            "Assigned %d workers, io_budget=%d: %s",
            self._n_workers,
            self._io_budget,
            {r: f"[{s},{e})" for r, (s, e) in assignments.items()},
        )
        return assignments