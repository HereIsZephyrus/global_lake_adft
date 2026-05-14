"""Single-process batch runners.

These runners own the serial read -> compute -> persist loop so that Engine can
remain a thin runtime selector instead of carrying execution details.
"""

from __future__ import annotations

from collections import defaultdict
import logging

from .domain import Calculator, LakeFilter, LakeTask
from .filter import IdSetFilter, RangeFilter
from .io import BatchReader, BatchWriter
from .protocol import RunReport, _iter_chunk_ranges, _iter_id_batches

log = logging.getLogger(__name__)


def _build_task(reader, hid: int, series_df: object, frozen_year_months: set[int]) -> LakeTask:
    if hasattr(reader, "build_task"):
        return reader.build_task(hid, series_df, frozen_year_months)
    return LakeTask(
        hylak_id=hid,
        series_df=series_df,
        frozen_year_months=frozenset(frozen_year_months),
        extra=None,
    )


class SingleProcessRunner:
    def __init__(
        self,
        reader: BatchReader,
        writer: BatchWriter,
        calculator: Calculator,
        *,
        algorithm: str,
        lake_filter: LakeFilter | None,
        chunk_size: int,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._calculator = calculator
        self._algorithm = algorithm
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size

    def run(self) -> RunReport:
        self._writer.ensure_schema(self._algorithm)
        max_id = self._reader.fetch_max_hylak_id()
        start, end = self._get_range()
        chunk_ranges = _iter_chunk_ranges(max_id, self._chunk_size, start=start, end=end)
        report = RunReport(total_chunks=len(chunk_ranges))

        for chunk_start, chunk_end in chunk_ranges:
            lake_map = self._reader.fetch_lake_area_chunk(chunk_start, chunk_end)
            if not lake_map:
                report.skipped_chunks += 1
                continue

            candidate_ids = set(lake_map.keys())
            if self._lake_filter:
                candidate_ids = candidate_ids & self._lake_filter(candidate_ids)
            done_ids = self._reader.fetch_done_ids(
                self._algorithm, chunk_start, chunk_end
            )
            pending_ids = candidate_ids - done_ids

            if not pending_ids:
                report.skipped_chunks += 1
                report.source_lakes += len(candidate_ids)
                report.skipped_lakes += len(candidate_ids)
                continue

            frozen_map = self._reader.fetch_frozen_year_months_chunk(
                chunk_start, chunk_end
            )
            report.source_lakes += len(candidate_ids)
            report.skipped_lakes += len(candidate_ids) - len(pending_ids)

            all_rows = self._compute_rows(
                pending_ids=pending_ids,
                lake_map=lake_map,
                frozen_map=frozen_map,
                report=report,
                error_chunk=(chunk_start, chunk_end),
            )
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

    def _get_range(self) -> tuple[int, int | None]:
        if self._lake_filter and isinstance(self._lake_filter, RangeFilter):
            return self._lake_filter.start, self._lake_filter.end
        return 0, None

    def _compute_rows(
        self,
        *,
        pending_ids: set[int],
        lake_map: dict[int, object],
        frozen_map: dict[int, set[int]],
        report: RunReport,
        error_chunk: tuple[int, int],
    ) -> dict[str, list[dict]]:
        all_rows: dict[str, list[dict]] = defaultdict(list)
        chunk_start, chunk_end = error_chunk
        for hid in sorted(pending_ids):
            task = _build_task(self._reader, hid, lake_map[hid], frozen_map.get(hid, set()))
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
        return all_rows


class SingleProcessIdBatchRunner:
    def __init__(
        self,
        reader: BatchReader,
        writer: BatchWriter,
        calculator: Calculator,
        *,
        algorithm: str,
        lake_filter: IdSetFilter,
        chunk_size: int,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._calculator = calculator
        self._algorithm = algorithm
        self._lake_filter = lake_filter
        self._chunk_size = chunk_size

    def run(self) -> RunReport:
        self._writer.ensure_schema(self._algorithm)
        sorted_ids = sorted(self._lake_filter.ids)
        id_batches = _iter_id_batches(sorted_ids, self._chunk_size)
        report = RunReport(total_chunks=len(id_batches))

        for batch_idx, id_batch in enumerate(id_batches):
            lake_map = self._reader.fetch_lake_area_by_ids(id_batch)
            if not lake_map:
                report.skipped_chunks += 1
                continue

            candidate_ids = set(lake_map.keys())
            done_ids = self._fetch_done_ids_by_batch(id_batch)
            pending_ids = candidate_ids - done_ids

            if not pending_ids:
                report.skipped_chunks += 1
                report.source_lakes += len(candidate_ids)
                report.skipped_lakes += len(candidate_ids)
                continue

            frozen_map = self._reader.fetch_frozen_year_months_by_ids(id_batch)
            report.source_lakes += len(candidate_ids)
            report.skipped_lakes += len(candidate_ids) - len(pending_ids)

            all_rows = self._compute_rows(
                pending_ids=pending_ids,
                lake_map=lake_map,
                frozen_map=frozen_map,
                report=report,
                error_chunk=(batch_idx, batch_idx + 1),
            )
            if any(all_rows.values()):
                self._writer.persist(dict(all_rows))
            report.processed_chunks += 1
            log.info(
                "ID batch %d/%d: ids=%d source=%d skip=%d success=%d error=%d",
                batch_idx + 1,
                len(id_batches),
                len(id_batch),
                len(candidate_ids),
                report.skipped_lakes,
                report.success_lakes,
                report.error_lakes,
            )

        return report

    def _fetch_done_ids_by_batch(self, id_batch: list[int]) -> set[int]:
        if not id_batch:
            return set()
        lo = min(id_batch)
        hi = max(id_batch) + 1
        done = self._reader.fetch_done_ids(self._algorithm, lo, hi)
        return done & set(id_batch)

    def _compute_rows(
        self,
        *,
        pending_ids: set[int],
        lake_map: dict[int, object],
        frozen_map: dict[int, set[int]],
        report: RunReport,
        error_chunk: tuple[int, int],
    ) -> dict[str, list[dict]]:
        all_rows: dict[str, list[dict]] = defaultdict(list)
        chunk_start, chunk_end = error_chunk
        for hid in sorted(pending_ids):
            task = _build_task(self._reader, hid, lake_map[hid], frozen_map.get(hid, set()))
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
        return all_rows
