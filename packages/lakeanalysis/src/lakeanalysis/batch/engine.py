"""Engine entry point for batch lake computation — dataset-first routing."""

from __future__ import annotations

import logging
import time

from .domain import Calculator, LakeFilter
from .io import BatchReader, BatchWriter
from .lake_dataset_factory import LakeDatasetFactory
from .lake_dataset_query import LakeDatasetQuery
from .protocol import RunReport, _iter_id_batches

log = logging.getLogger(__name__)


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
        dataset_factory: LakeDatasetFactory | None = None,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._calculator = calculator
        self._algorithm = algorithm
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size
        self._dataset_factory = dataset_factory

    def _maybe_truncate(self) -> None:
        if self._lake_filter is None:
            self._writer.truncate_run_status(self._algorithm)

    def _resolve_candidate_ids(self) -> list[int]:
        """Resolve the actual computation domain before chunking.

        The batch engine computes only lakes present in ``area_quality``.
        User-supplied ``lake_filter`` and algorithm-specific done-state are
        both applied before chunk construction so that single-process and MPI
        scheduling operate on the same precise ID universe.
        """
        started = time.perf_counter()
        candidate_ids = self._reader.fetch_quality_ids()
        if self._lake_filter is not None:
            candidate_ids = self._lake_filter(candidate_ids)
        if candidate_ids:
            done_ids = self._reader.fetch_done_ids(
                self._algorithm,
                min(candidate_ids),
                max(candidate_ids) + 1,
            )
            candidate_ids -= done_ids
        resolved = sorted(candidate_ids)
        log.info(
            "Resolved %d candidate lakes for algorithm=%s in %.3fs",
            len(resolved),
            self._algorithm,
            time.perf_counter() - started,
        )
        return resolved

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
        if rank == 0:
            started = time.perf_counter()
            self._writer.ensure_schema(self._algorithm)
            sorted_ids = self._resolve_candidate_ids()
            manager = Manager(
                comm, size, self._lake_filter, self._writer,
            )
            result = manager.run_dataset_id_batch(
                sorted_ids, self._chunk_size, self._algorithm,
            )
            self._maybe_truncate()
            log.info(
                "MPI manager finished algorithm=%s size=%d elapsed=%.3fs",
                self._algorithm,
                size,
                time.perf_counter() - started,
            )
            return result

        assignments = comm.bcast(None, root=0)
        assignment = assignments.get(rank) if assignments is not None else None
        worker = Worker(
            rank, self._algorithm,
            self._calculator, self._chunk_size,
        )
        worker.run_dataset_id_batch(comm, self._dataset_factory, assignment)
        comm.bcast(None, root=0)
        return None

    # ── query construction ─────────────────────────────────────────────

    def _build_queries(self):
        """Yield ``LakeDatasetQuery`` objects over exact quality-domain ID batches."""
        sorted_ids = self._resolve_candidate_ids()
        for id_batch in _iter_id_batches(sorted_ids, self._chunk_size):
            yield LakeDatasetQuery(
                algorithm=self._algorithm,
                id_subset=frozenset(id_batch),
                require_quality=False,
                exclude_done=False,
            )
