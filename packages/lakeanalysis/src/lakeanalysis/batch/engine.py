"""Engine entry point and ABC definitions for batch lake computation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
import logging
from typing import Any, Iterable

import pandas as pd

from lakesource.provider import LakeProvider

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
        self.start = start
        self.end = end

    def __call__(self, hylak_ids: Iterable[int]) -> set[int]:
        ids = set(hylak_ids)
        ids = {i for i in ids if i >= self.start}
        if self.end is not None:
            ids = {i for i in ids if i < self.end}
        return ids


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
        provider: LakeProvider,
        calculator: Calculator,
        *,
        algorithm: str = "quantile",
        lake_filter: LakeFilter | None = None,
        chunk_size: int = 10_000,
        io_budget: int = 4,
    ) -> None:
        self._provider = provider
        self._calculator = calculator
        self._algorithm = algorithm
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size
        self._io_budget = io_budget

    def _get_range(self) -> tuple[int, int | None]:
        if self._lake_filter and isinstance(self._lake_filter, RangeFilter):
            return self._lake_filter.start, self._lake_filter.end
        return 0, None

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
            self._provider.ensure_schema(self._algorithm)
            max_id = self._provider.fetch_max_hylak_id()
            manager = Manager(comm, size, self._io_budget, self._lake_filter)
            return manager.run(max_id, self._chunk_size)

        assignments = comm.bcast(None, root=0)
        worker = Worker(
            rank, self._provider, self._algorithm,
            self._calculator, self._chunk_size,
        )
        worker.run(comm, assignments)
        comm.bcast(None, root=0)
        return None

    def _run_single(self) -> RunReport:
        self._provider.ensure_schema(self._algorithm)
        max_id = self._provider.fetch_max_hylak_id()
        start, end = self._get_range()
        chunk_ranges = _iter_chunk_ranges(max_id, self._chunk_size, start=start, end=end)
        report = RunReport(total_chunks=len(chunk_ranges))

        for chunk_start, chunk_end in chunk_ranges:
            lake_map = self._provider.fetch_lake_area_chunk(chunk_start, chunk_end)
            if not lake_map:
                report.skipped_chunks += 1
                continue

            candidate_ids = set(lake_map.keys())
            if self._lake_filter:
                candidate_ids = candidate_ids & self._lake_filter(candidate_ids)
            done_ids = self._provider.fetch_done_ids(
                self._algorithm, chunk_start, chunk_end
            )
            pending_ids = candidate_ids - done_ids

            if not pending_ids:
                report.skipped_chunks += 1
                report.source_lakes += len(candidate_ids)
                report.skipped_lakes += len(candidate_ids)
                continue

            frozen_map = self._provider.fetch_frozen_year_months_chunk(
                chunk_start, chunk_end
            )
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
                self._provider.persist(dict(all_rows))
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
