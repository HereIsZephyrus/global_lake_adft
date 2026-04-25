"""Worker: state-machine driven lake computation with own reader/calculator/writer.

State machine:
    pending ──TRIGGER_READ──▶ reading ──(auto)──▶ calculating ──(auto)──▶ pending
    pending ──TRIGGER_WRITE──▶ writing ──(auto)──▶ pending / done

IO slots are occupied when TRIGGER is sent, released when the IO phase ends:
  - TRIGGER_READ sent → io_active += 1; CALCULATING received → io_active -= 1
  - TRIGGER_WRITE sent → io_active += 1; PENDING(prev=writing) received → io_active -= 1
"""

from __future__ import annotations

from collections import defaultdict
import logging

from .engine import LakeTask
from .protocol import TAG_STATUS, TAG_TRIGGER, TRIGGER_READ, TRIGGER_WRITE, WorkerState, _iter_chunk_ranges

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        rank: int,
        reader,
        calculator,
        writer,
        chunk_size: int,
    ) -> None:
        self._rank = rank
        self._reader = reader
        self._calculator = calculator
        self._writer = writer
        self._chunk_size = chunk_size

    def run(self, comm, assignments: dict[int, tuple[int, int]]) -> None:
        start, end = assignments[self._rank]

        if start >= end:
            self._send(comm, WorkerState.DONE, {
                "source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0,
            })
            return

        chunk_ranges = _iter_chunk_ranges(end - 1, self._chunk_size, end)
        total_stats = {"source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0}

        self._send(comm, WorkerState.PENDING, {})

        for cs, ce in chunk_ranges:
            comm.recv(source=0, tag=TAG_TRIGGER)
            self._send(comm, WorkerState.READING, {})

            lake_map = self._reader.fetch_lake_map(cs, ce)
            if not lake_map:
                self._send(comm, WorkerState.CALCULATING, {
                    "source": 0, "skipped": 0, "success": 0, "error": 0,
                })
                self._send(comm, WorkerState.PENDING, {})
                comm.recv(source=0, tag=TAG_TRIGGER)
                self._send(comm, WorkerState.WRITING, {})
                self._send(comm, WorkerState.PENDING, {})
                total_stats["chunks"] += 1
                continue

            frozen_map = self._reader.fetch_frozen_map(cs, ce)
            done_ids = self._reader.fetch_done_ids(cs, ce)
            candidate_ids = set(lake_map.keys())
            pending_ids = candidate_ids - done_ids

            chunk_stats = {
                "source": len(candidate_ids),
                "skipped": len(candidate_ids) - len(pending_ids),
                "success": 0,
                "error": 0,
            }

            self._send(comm, WorkerState.CALCULATING, chunk_stats)

            all_rows: dict[str, list[dict]] = defaultdict(list)
            for hid in sorted(pending_ids):
                task = LakeTask(
                    hylak_id=hid,
                    series_df=lake_map[hid],
                    frozen_year_months=frozenset(frozen_map.get(hid, set())),
                )
                try:
                    result = self._calculator.run(task)
                    for table, rows in self._calculator.result_to_rows(result).items():
                        all_rows[table].extend(rows)
                    chunk_stats["success"] += 1
                except Exception as exc:
                    for table, rows in self._calculator.error_to_rows(
                        hid, exc, cs, ce
                    ).items():
                        all_rows[table].extend(rows)
                    chunk_stats["error"] += 1

            self._send(comm, WorkerState.PENDING, {})

            comm.recv(source=0, tag=TAG_TRIGGER)
            self._send(comm, WorkerState.WRITING, {})

            if any(all_rows.values()):
                self._writer.persist(dict(all_rows))

            for k in ("source", "skipped", "success", "error"):
                total_stats[k] += chunk_stats[k]
            total_stats["chunks"] += 1

            log.info(
                "Worker %d chunk [%d, %d): source=%d skip=%d success=%d error=%d",
                self._rank,
                cs,
                ce,
                chunk_stats["source"],
                chunk_stats["skipped"],
                chunk_stats["success"],
                chunk_stats["error"],
            )

            self._send(comm, WorkerState.PENDING, {})

        self._send(comm, WorkerState.DONE, total_stats)

    def _send(self, comm, state: str, stats: dict) -> None:
        comm.send((state, stats), dest=0, tag=TAG_STATUS)