"""Engine entry point for batch lake computation — dataset-first routing."""

from __future__ import annotations

from .domain import Calculator, LakeFilter
from .filter import IdSetFilter, RangeFilter
from .io import BatchReader, BatchWriter
from .lake_dataset_factory import LakeDatasetFactory
from .lake_dataset_query import LakeDatasetQuery
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
        dataset_factory: LakeDatasetFactory | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._calculator = calculator
        self._algorithm = algorithm
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size
        self._io_budget = io_budget
        self._dataset_factory = dataset_factory

    def _maybe_truncate(self) -> None:
        if self._lake_filter is None:
            self._writer.truncate_run_status(self._algorithm)

    def run(self) -> RunReport | None:
        try:
            from mpi4py import MPI

            comm = MPI.COMM_WORLD
            size = comm.Get_size()
            rank = comm.Get_rank()
            has_mpi = True
        except ImportError:
            size = 1
            rank = 0
            comm = None
            has_mpi = False

        if has_mpi and size > 1:
            return self._run_mpi(comm, size, rank)

        return self._run_single_process()

    # ── single-process path ────────────────────────────────────────────

    def _run_single_process(self) -> RunReport:
        factory = self._dataset_factory
        if factory is None:
            return RunReport(total_chunks=0)

        report = RunReport()

        queries = list(self._build_queries())
        report.total_chunks = len(queries)

        for query in queries:
            from .single_process import SingleProcessLakeDatasetRunner
            runner = SingleProcessLakeDatasetRunner(
                factory,
                query,
                self._writer,
                self._calculator,
                algorithm=self._algorithm,
            )
            chunk_report = runner.run()
            for attr in ("source_lakes", "skipped_lakes", "success_lakes",
                         "error_lakes", "processed_chunks", "skipped_chunks"):
                setattr(report, attr, getattr(report, attr) + getattr(chunk_report, attr))

        self._maybe_truncate()
        return report

    # ── MPI path ────────────────────────────────────────────────────────

    def _run_mpi(self, comm, size: int, rank: int) -> RunReport | None:
        from .manager import Manager
        from .worker import Worker

        if self._lake_filter is not None and isinstance(self._lake_filter, IdSetFilter):
            sorted_ids = sorted(self._lake_filter.ids)

            if rank == 0:
                self._writer.ensure_schema(self._algorithm)
                manager = Manager(
                    comm, size, self._io_budget,
                    self._lake_filter, self._writer,
                )
                result = manager.run_dataset_id_batch(
                    sorted_ids, self._chunk_size, self._algorithm,
                )
                self._maybe_truncate()
                return result

            queries = comm.bcast(None, root=0)
            worker = Worker(
                rank, self._reader, self._algorithm,
                self._calculator, self._chunk_size,
            )
            worker.run_dataset_id_batch(comm, self._dataset_factory, queries)
            comm.bcast(None, root=0)
            return None

        # Range-filter or full-sweep MPI:
        if rank == 0:
            self._writer.ensure_schema(self._algorithm)
            if isinstance(self._lake_filter, RangeFilter):
                start = self._lake_filter.start
                end = self._lake_filter.end or (self._reader.fetch_max_hylak_id() + 1)
                sorted_ids = list(range(start, end))
            else:
                max_id = self._reader.fetch_max_hylak_id()
                sorted_ids = list(range(0, max_id + 1))

            manager = Manager(
                comm, size, self._io_budget, self._lake_filter, self._writer,
            )
            result = manager.run_dataset_id_batch(
                sorted_ids, self._chunk_size, self._algorithm,
            )
            self._maybe_truncate()
            return result

        queries = comm.bcast(None, root=0)
        worker = Worker(
            rank, self._reader, self._algorithm,
            self._calculator, self._chunk_size,
        )
        worker.run_dataset_id_batch(comm, self._dataset_factory, queries)
        comm.bcast(None, root=0)
        return None

    # ── query construction ─────────────────────────────────────────────

    def _build_queries(self):
        """Yield ``LakeDatasetQuery`` objects for the configured filter."""
        if self._lake_filter is not None and isinstance(self._lake_filter, IdSetFilter):
            sorted_ids = sorted(self._lake_filter.ids)
            if sorted_ids:
                yield LakeDatasetQuery(
                    algorithm=self._algorithm,
                    id_range=(min(sorted_ids), max(sorted_ids) + 1),
                    require_quality=False,
                    exclude_done=True,
                )
            return

        if self._lake_filter is not None and isinstance(self._lake_filter, RangeFilter):
            start = self._lake_filter.start
            end = self._lake_filter.end or self._reader.fetch_max_hylak_id()
            for cs in range(start, end, self._chunk_size):
                yield LakeDatasetQuery(
                    algorithm=self._algorithm,
                    id_range=(cs, min(cs + self._chunk_size, end)),
                    require_quality=False,
                    exclude_done=True,
                )
            return

        max_id = self._reader.fetch_max_hylak_id()
        for cs in range(0, max_id, self._chunk_size):
            yield LakeDatasetQuery(
                algorithm=self._algorithm,
                id_range=(cs, min(cs + self._chunk_size, max_id)),
                require_quality=False,
                exclude_done=True,
            )
