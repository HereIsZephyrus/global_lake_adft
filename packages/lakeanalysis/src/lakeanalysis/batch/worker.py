"""Worker: state-machine driven lake computation with own provider/calculator.

State machine:
    pending ──TRIGGER_READ──▶ reading ──(auto)──▶ calculating ──(auto)──▶ pending

Workers send computed data to rank 0 via TAG_DATA; rank 0 handles all writes.
"""

from __future__ import annotations

from collections import defaultdict
import logging

from lakesource.provider import LakeProvider

from .engine import LakeTask
from .protocol import TAG_STATUS, TAG_TRIGGER, TAG_DATA, TRIGGER_READ, WorkerState, _iter_chunk_ranges

log = logging.getLogger(__name__)


class Worker:
    def __init__(
        self,
        rank: int,
        provider: LakeProvider,
        algorithm: str,
        calculator,
        chunk_size: int,
    ) -> None:
        self._rank = rank
        self._provider = provider
        self._algorithm = algorithm
        self._calculator = calculator
        self._chunk_size = chunk_size

    def run(self, comm, assignments: dict[int, tuple[int, int]]) -> None:
        start, end = assignments[self._rank]

        if start >= end:
            self._send(comm, WorkerState.DONE, {
                "source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0,
            })
            return

        chunk_ranges = _iter_chunk_ranges(end - 1, self._chunk_size, start=start, end=end)
        total_stats = {"source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0}

        self._send(comm, WorkerState.PENDING, {})

        for i, (cs, ce) in enumerate(chunk_ranges):
            is_last = (i == len(chunk_ranges) - 1)
            comm.recv(source=0, tag=TAG_TRIGGER)
            self._send(comm, WorkerState.READING, {})

            lake_map = self._provider.fetch_lake_area_chunk(cs, ce)
            if not lake_map:
                total_stats["chunks"] += 1
                if is_last:
                    self._send(comm, WorkerState.DONE, total_stats)
                else:
                    self._send(comm, WorkerState.PENDING, {})
                continue

            frozen_map = self._provider.fetch_frozen_year_months_chunk(cs, ce)
            done_ids = self._provider.fetch_done_ids(self._algorithm, cs, ce)
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

            if any(all_rows.values()):
                comm.send(dict(all_rows), dest=0, tag=TAG_DATA)

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

            if is_last:
                self._send(comm, WorkerState.DONE, total_stats)
            else:
                self._send(comm, WorkerState.PENDING, {})

    def run_id_batch(
        self, comm, assignments: dict[int, list[list[int]]]
    ) -> None:
        id_batches = assignments.get(self._rank, [])

        if not id_batches:
            self._send(comm, WorkerState.DONE, {
                "source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0,
            })
            return

        total_stats = {"source": 0, "skipped": 0, "success": 0, "error": 0, "chunks": 0}

        self._send(comm, WorkerState.PENDING, {})

        for i, id_batch in enumerate(id_batches):
            is_last = (i == len(id_batches) - 1)
            comm.recv(source=0, tag=TAG_TRIGGER)
            self._send(comm, WorkerState.READING, {})

            lake_map = self._provider.fetch_lake_area_by_ids(id_batch)
            if not lake_map:
                total_stats["chunks"] += 1
                if is_last:
                    self._send(comm, WorkerState.DONE, total_stats)
                else:
                    self._send(comm, WorkerState.PENDING, {})
                continue

            frozen_map = self._provider.fetch_frozen_year_months_by_ids(id_batch)
            lo = min(id_batch)
            hi = max(id_batch) + 1
            done_ids = self._provider.fetch_done_ids(self._algorithm, lo, hi) & set(id_batch)
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
                        hid, exc, lo, hi
                    ).items():
                        all_rows[table].extend(rows)
                    chunk_stats["error"] += 1

            if any(all_rows.values()):
                comm.send(dict(all_rows), dest=0, tag=TAG_DATA)

            for k in ("source", "skipped", "success", "error"):
                total_stats[k] += chunk_stats[k]
            total_stats["chunks"] += 1

            log.info(
                "Worker %d id-batch %d/%d: ids=%d source=%d skip=%d success=%d error=%d",
                self._rank,
                i + 1,
                len(id_batches),
                len(id_batch),
                chunk_stats["source"],
                chunk_stats["skipped"],
                chunk_stats["success"],
                chunk_stats["error"],
            )

            if is_last:
                self._send(comm, WorkerState.DONE, total_stats)
            else:
                self._send(comm, WorkerState.PENDING, {})

    def _send(self, comm, state: str, stats: dict) -> None:
        comm.send((state, stats), dest=0, tag=TAG_STATUS)
