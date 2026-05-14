"""Single-process batch runner — dataset-first path.

The ``SingleProcessLakeDatasetRunner`` is the only single-process runner.
It builds a ``LakeDataset`` via the factory, then delegates to
``Calculator.run_dataset`` for per-lake computation and row aggregation.
"""

from __future__ import annotations

import logging

from .domain import Calculator
from .io import BatchWriter
from .lake_dataset_factory import LakeDatasetFactory
from .lake_dataset_query import LakeDatasetQuery
from .protocol import RunReport

log = logging.getLogger(__name__)


class SingleProcessLakeDatasetRunner:
    def __init__(
        self,
        dataset_factory: LakeDatasetFactory,
        dataset_query: LakeDatasetQuery,
        writer: BatchWriter,
        calculator: Calculator,
        *,
        algorithm: str,
    ) -> None:
        self._dataset_factory = dataset_factory
        self._dataset_query = dataset_query
        self._writer = writer
        self._calculator = calculator
        self._algorithm = algorithm

    def run(self) -> RunReport:
        self._writer.ensure_schema(self._algorithm)
        dataset = self._dataset_factory.build(self._dataset_query)
        report = RunReport(total_chunks=1)
        report.source_lakes = len(dataset)

        if len(dataset) == 0:
            report.skipped_chunks = 1
            return report

        rows_by_table, success_lakes, error_lakes = self._calculator.run_dataset(
            dataset,
            error_chunk=(0, 1),
        )
        if any(rows_by_table.values()):
            self._writer.persist(dict(rows_by_table))
        report.processed_chunks = 1
        report.success_lakes = success_lakes
        report.error_lakes = error_lakes
        return report
