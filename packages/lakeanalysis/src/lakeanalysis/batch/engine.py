"""Engine entry point and ABC definitions for batch lake computation."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from .io import BatchReader, BatchWriter
from .protocol import RunReport


@dataclass(frozen=True)
class LakeTask:
    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: frozenset[int]
    extra: dict[str, Any] | None = None


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


class IdSetFilter(LakeFilter):
    def __init__(self, ids: set[int] | list[int]) -> None:
        self._ids = set(ids)

    def __call__(self, hylak_ids: Iterable[int]) -> set[int]:
        return self._ids & set(hylak_ids)

    @property
    def ids(self) -> set[int]:
        return self._ids


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
        reader: BatchReader,
        writer: BatchWriter,
        calculator: Calculator,
        *,
        algorithm: str = "quantile",
        lake_filter: LakeFilter | None = None,
        chunk_size: int = 10_000,
        io_budget: int = 4,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._calculator = calculator
        self._algorithm = algorithm
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size
        self._io_budget = io_budget

    def _is_id_batch_mode(self) -> bool:
        return isinstance(self._lake_filter, IdSetFilter)

    def _get_range(self) -> tuple[int, int | None]:
        if self._lake_filter and isinstance(self._lake_filter, RangeFilter):
            return self._lake_filter.start, self._lake_filter.end
        return 0, None

    def _maybe_truncate(self) -> None:
        """Truncate run_status after a full (unfiltered) run completes."""
        if self._lake_filter is None:
            self._writer.truncate_run_status(self._algorithm)

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

        if self._is_id_batch_mode():
            if size <= 1:
                from .single_process import SingleProcessIdBatchRunner

                result = SingleProcessIdBatchRunner(
                    self._reader,
                    self._writer,
                    self._calculator,
                    algorithm=self._algorithm,
                    lake_filter=self._lake_filter,
                    chunk_size=self._chunk_size,
                ).run()
                self._maybe_truncate()
                return result
            from .manager import Manager
            from .worker import Worker

            if rank == 0:
                self._writer.ensure_schema(self._algorithm)
                sorted_ids = sorted(self._lake_filter.ids)
                manager = Manager(comm, size, self._io_budget, self._lake_filter, self._writer)
                result = manager.run_id_batch(sorted_ids, self._chunk_size)
                self._maybe_truncate()
                return result

            assignments = comm.bcast(None, root=0)
            worker = Worker(
                rank, self._reader, self._algorithm,
                self._calculator, self._chunk_size,
            )
            worker.run_id_batch(comm, assignments)
            comm.bcast(None, root=0)
            return None

        if size <= 1:
            from .single_process import SingleProcessRunner

            result = SingleProcessRunner(
                self._reader,
                self._writer,
                self._calculator,
                algorithm=self._algorithm,
                lake_filter=self._lake_filter,
                chunk_size=self._chunk_size,
            ).run()
            self._maybe_truncate()
            return result

        from .manager import Manager
        from .worker import Worker

        if rank == 0:
            self._writer.ensure_schema(self._algorithm)
            max_id = self._reader.fetch_max_hylak_id()
            manager = Manager(comm, size, self._io_budget, self._lake_filter, self._writer)
            result = manager.run(max_id, self._chunk_size)
            self._maybe_truncate()
            return result

        assignments = comm.bcast(None, root=0)
        worker = Worker(
            rank, self._reader, self._algorithm,
            self._calculator, self._chunk_size,
        )
        worker.run(comm, assignments)
        comm.bcast(None, root=0)
        return None
