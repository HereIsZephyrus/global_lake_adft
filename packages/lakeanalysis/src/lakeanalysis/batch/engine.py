"""Engine entry point for batch lake computation."""

from __future__ import annotations

from .domain import Calculator, LakeFilter, LakeTask
from .filter import IdSetFilter, RangeFilter
from .io import BatchReader, BatchWriter
from .protocol import RunReport


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
