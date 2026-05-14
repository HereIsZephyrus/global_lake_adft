"""Worker: state-machine driven lake computation with own provider/calculator.

State machine:
    pending ──TRIGGER_READ──▶ reading ──(auto)──▶ calculating ──(auto)──▶ pending

Workers send computed data to rank 0 via TAG_DATA; rank 0 handles all writes.
"""

from __future__ import annotations

import logging

from .io import BatchReader
from .protocol import TAG_STATUS, TAG_TRIGGER, TAG_DATA, WorkerState

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        rank: int,
        reader: BatchReader,
        algorithm: str,
        calculator,
        chunk_size: int,
    ) -> None:
        self._rank = rank
        self._reader = reader
        self._algorithm = algorithm
        self._calculator = calculator
        self._chunk_size = chunk_size

    def _send(self, comm, state: str, stats: dict) -> None:
        comm.send((state, stats), dest=0, tag=TAG_STATUS)

    def run_dataset_id_batch(
        self,
        comm,
        factory,
        queries: list,
    ) -> None:
        if not queries:
            self._send(comm, WorkerState.DONE, {
                "source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0,
            })
            return

        total_stats = {"source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0}
        self._send(comm, WorkerState.PENDING, {})

        for i, query in enumerate(queries):
            is_last = (i == len(queries) - 1)
            comm.recv(source=0, tag=TAG_TRIGGER)
            self._send(comm, WorkerState.READING, {})

            dataset = factory.build(query)

            self._send(comm, WorkerState.CALCULATING, {
                "source": len(dataset),
                "skipped": 0,
                "success": 0,
                "error": 0,
            })

            rows_by_table, success_lakes, error_lakes = self._calculator.run_dataset(
                dataset,
                error_chunk=(0, 1),
            )

            if any(rows_by_table.values()):
                comm.send(dict(rows_by_table), dest=0, tag=TAG_DATA)

            total_stats["source"] += len(dataset)
            total_stats["success"] += success_lakes
            total_stats["error"] += error_lakes
            total_stats["chunks"] += 1

            if is_last:
                self._send(comm, WorkerState.DONE, total_stats)
            else:
                self._send(comm, WorkerState.PENDING, {})
