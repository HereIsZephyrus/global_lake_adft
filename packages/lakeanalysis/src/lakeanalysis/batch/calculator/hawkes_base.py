"""Shared batch-layer abstractions for Hawkes-based calculators."""

from __future__ import annotations

from dataclasses import dataclass, field

from lakeanalysis.hawkes import (
    HawkesCoreResult,
    build_error_summary,
    build_hawkes_result_row,
    make_hawkes_run_status_row,
)

from .. import Calculator

RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"


@dataclass(frozen=True)
class HawkesResult:
    """Unified batch-level output for Hawkes calculators."""

    core: HawkesCoreResult
    return_level_rows: list[dict] = field(default_factory=list)
    extra_rows_by_table: dict[str, list[dict]] = field(default_factory=dict)


class HawkesCalculator(Calculator):
    """Shared row-shaping and error handling for Hawkes calculators."""

    _table_prefix: str
    _return_levels_table: str | None = None

    def __init__(
        self,
        *,
        hawkes_window_months: float = 4.0,
        min_event_rate: float = 0.01,
        max_event_rate: float = 0.30,
        min_relative_amplitude: float = 0.05,
        min_median_severity: float = 1.0,
        monthly_significance_quantile: float = 0.95,
    ) -> None:
        self._hawkes_window_months = hawkes_window_months
        self._min_event_rate = min_event_rate
        self._max_event_rate = max_event_rate
        self._min_relative_amplitude = min_relative_amplitude
        self._min_median_severity = min_median_severity
        self._monthly_significance_quantile = monthly_significance_quantile

    def result_to_rows(self, result: HawkesResult) -> dict[str, list[dict]]:
        error_msg = result.core.summary.get("error_message")
        success = error_msg is None
        rows = {
            f"{self._table_prefix}_results": [
                build_hawkes_result_row(result.core.summary)
            ],
            f"{self._table_prefix}_lrt": result.core.lrt_rows,
            f"{self._table_prefix}_transition_monthly": (
                result.core.transition_monthly_rows
            ),
            f"{self._table_prefix}_run_status": [
                make_hawkes_run_status_row(
                    hylak_id=result.core.summary["hylak_id"],
                    status=RUN_STATUS_DONE if success else RUN_STATUS_ERROR,
                    error_message=error_msg,
                )
            ],
        }
        if self._return_levels_table is not None and (
            result.return_level_rows
            or self._return_levels_table in result.extra_rows_by_table
        ):
            rows[self._return_levels_table] = result.return_level_rows
        rows.update(result.extra_rows_by_table)
        return rows

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        del chunk_start, chunk_end
        error_summary = build_error_summary(hylak_id, str(error))
        return {
            f"{self._table_prefix}_run_status": [
                make_hawkes_run_status_row(
                    hylak_id=hylak_id,
                    status=RUN_STATUS_ERROR,
                    error_message=str(error)[:500],
                )
            ],
            f"{self._table_prefix}_results": [
                build_hawkes_result_row(error_summary)
            ],
        }

    def _make_result(
        self,
        core: HawkesCoreResult,
        *,
        return_level_rows: list[dict] | None = None,
        extra_rows_by_table: dict[str, list[dict]] | None = None,
    ) -> HawkesResult:
        return HawkesResult(
            core=core,
            return_level_rows=return_level_rows or [],
            extra_rows_by_table=extra_rows_by_table or {},
        )
