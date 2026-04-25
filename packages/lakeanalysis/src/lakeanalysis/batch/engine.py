"""Engine entry point and ABC definitions for batch lake computation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Any, Iterable

import pandas as pd

from .protocol import RunReport, _iter_chunk_ranges

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LakeTask:
    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: frozenset[int]


class LakeFilter(ABC):
    @abstractmethod
    def __call__(self, hylak_ids: Iterable[int]) -> set[int]: ...


class RangeFilter(LakeFilter):
    def __init__(self, start: int = 0, end: int | None = None) -> None:
        self._start = start
        self._end = end

    def __call__(self, hylak_ids: Iterable[int]) -> set[int]:
        ids = set(hylak_ids)
        ids = {i for i in ids if i >= self._start}
        if self._end is not None:
            ids = {i for i in ids if i < self._end}
        return ids


class Reader(ABC):
    @abstractmethod
    def fetch_lake_map(self, chunk_start: int, chunk_end: int) -> dict[int, pd.DataFrame]: ...

    def fetch_frozen_map(self, chunk_start: int, chunk_end: int) -> dict[int, set[int]]:
        return {}

    @abstractmethod
    def fetch_done_ids(self, chunk_start: int, chunk_end: int) -> set[int]: ...

    @abstractmethod
    def max_hylak_id(self) -> int: ...

    @abstractmethod
    def ensure_schema(self) -> None: ...


class Writer(ABC):
    @abstractmethod
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None: ...


class Calculator(ABC):
    @abstractmethod
    def run(self, task: LakeTask) -> Any: ...

    @abstractmethod
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]: ...

    @abstractmethod
    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]: ...


class Engine:
    def __init__(
        self,
        reader: Reader,
        writer: Writer,
        calculator: Calculator,
        *,
        lake_filter: LakeFilter | None = None,
        chunk_size: int = 10_000,
        limit_id: int | None = None,
        io_budget: int = 4,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._calculator = calculator
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size
        self._limit_id = limit_id
        self._io_budget = io_budget

    def run(self) -> RunReport | None:
        try:
            from mpi4py import MPI

            comm = MPI.COMM_WORLD
            size = comm.Get_size()
            rank = comm.Get_rank()
        except ImportError:
            size = 1
            rank = 0
            comm = None

        if size <= 1:
            return self._run_single()

        from .manager import Manager
        from .worker import Worker

        if rank == 0:
            self._reader.ensure_schema()
            max_id = self._reader.max_hylak_id()
            manager = Manager(comm, size, self._io_budget)
            return manager.run(max_id, self._chunk_size, self._limit_id)

        assignments = comm.bcast(None, root=0)
        worker = Worker(
            rank, self._reader, self._calculator,
            self._writer, self._chunk_size,
        )
        worker.run(comm, assignments)
        comm.bcast(None, root=0)
        return None

    def _run_single(self) -> RunReport:
        self._reader.ensure_schema()
        max_id = self._reader.max_hylak_id()
        chunk_ranges = _iter_chunk_ranges(max_id, self._chunk_size, self._limit_id)
        report = RunReport(total_chunks=len(chunk_ranges))

        for chunk_start, chunk_end in chunk_ranges:
            lake_map = self._reader.fetch_lake_map(chunk_start, chunk_end)
            if not lake_map:
                report.skipped_chunks += 1
                continue

            candidate_ids = set(lake_map.keys())
            if self._lake_filter:
                candidate_ids = candidate_ids & self._lake_filter(candidate_ids)
            done_ids = self._reader.fetch_done_ids(chunk_start, chunk_end)
            pending_ids = candidate_ids - done_ids

            if not pending_ids:
                report.skipped_chunks += 1
                report.source_lakes += len(candidate_ids)
                report.skipped_lakes += len(candidate_ids)
                continue

            frozen_map = self._reader.fetch_frozen_map(chunk_start, chunk_end)
            report.source_lakes += len(candidate_ids)
            report.skipped_lakes += len(candidate_ids) - len(pending_ids)

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
                    report.success_lakes += 1
                except Exception as exc:
                    for table, rows in self._calculator.error_to_rows(
                        hid, exc, chunk_start, chunk_end
                    ).items():
                        all_rows[table].extend(rows)
                    report.error_lakes += 1

            if any(all_rows.values()):
                self._writer.persist(dict(all_rows))
            report.processed_chunks += 1
            log.info(
                "Chunk [%d, %d): source=%d skip=%d success=%d error=%d",
                chunk_start,
                chunk_end,
                len(candidate_ids),
                report.skipped_lakes,
                report.success_lakes,
                report.error_lakes,
            )

        return report