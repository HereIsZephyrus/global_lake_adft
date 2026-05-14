"""EOTHawkesCalculator: EOT event extraction -> Hawkes fit.

Uses the EOT (Extremes Over Threshold) pathway to extract events, then fits
a bivariate Hawkes process via the shared ``run_hawkes_pipeline`` orchestrator.
Defaults to RunsDeclustering to decluster consecutive exceedances;
NoDeclustering available for comparison.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from lakeanalysis.eot import NoDeclustering, RunsDeclustering
from lakeanalysis.hawkes import (
    HawkesQCFailError,
    RunHawkesPipelineResult,
    build_error_summary,
    build_hawkes_result_row,
    build_qc_fail_summary,
    build_events_from_eot,
    make_hawkes_run_status_row,
    run_hawkes_pipeline,
)

from ..domain import Calculator, LakeTask
from ..lake_dataset import LakeDataset

log = logging.getLogger(__name__)

RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"

_TABLE_PREFIX = "eot_hawkes"


class EOTHawkesCalculator(Calculator):
    """Batch calculator: EOT event extraction -> Hawkes process fitting.

    Pipeline:
        1. Build dual-type event series from EOT (high/low tail quantile threshold)
        2. QC check + fit + LRT + decomposition via ``run_hawkes_pipeline``
    """

    def __init__(
        self,
        *,
        threshold_quantile: float = 0.90,
        hawkes_window_months: float = 4.0,
        min_event_rate: float = 0.01,
        max_event_rate: float = 0.30,
        min_relative_amplitude: float = 0.05,
        min_median_severity: float = 1.0,
        monthly_significance_quantile: float = 0.95,
        decluster_run_length: int | None = 1,
    ) -> None:
        self._threshold_quantile = threshold_quantile
        self._hawkes_window_months = hawkes_window_months
        self._min_event_rate = min_event_rate
        self._max_event_rate = max_event_rate
        self._min_relative_amplitude = min_relative_amplitude
        self._min_median_severity = min_median_severity
        self._monthly_significance_quantile = monthly_significance_quantile
        self._decluster_run_length = decluster_run_length

    def _compute_lake(self, task: LakeTask) -> RunHawkesPipelineResult:
        hylak_id = task.hylak_id
        series_df = task.series_df
        frozen = set(task.frozen_year_months) if task.frozen_year_months else None

        try:
            if self._decluster_run_length is None or self._decluster_run_length <= 0:
                decluster_strategy = NoDeclustering()
            else:
                decluster_strategy = RunsDeclustering(
                    run_length=self._decluster_run_length
                )
            event_series = build_events_from_eot(
                series_df,
                threshold_quantile=self._threshold_quantile,
                frozen_year_months=frozen,
                declustering_strategy=decluster_strategy,
            )
            events_table = event_series.events_table

            return run_hawkes_pipeline(
                event_series,
                events_table,
                series_df,
                hylak_id=hylak_id,
                threshold_quantile=self._threshold_quantile,
                hawkes_window_months=self._hawkes_window_months,
                min_event_rate=self._min_event_rate,
                max_event_rate=self._max_event_rate,
                min_relative_amplitude=self._min_relative_amplitude,
                min_median_severity=self._min_median_severity,
                monthly_significance_quantile=self._monthly_significance_quantile,
            )
        except HawkesQCFailError as e:
            summary = build_qc_fail_summary(
                hylak_id, e.qc, str(e), self._threshold_quantile
            )
            return RunHawkesPipelineResult(
                summary=summary, lrt_rows=[], transition_monthly_rows=[]
            )
        except Exception as exc:
            log.debug("EOT-Hawkes failed for hylak_id=%d: %s", task.hylak_id, exc)
            error_summary = build_error_summary(
                hylak_id, str(exc), self._threshold_quantile
            )
            return RunHawkesPipelineResult(
                summary=error_summary, lrt_rows=[], transition_monthly_rows=[]
            )

    def run_dataset(
        self,
        dataset: LakeDataset,
        *,
        error_chunk: tuple[int, int] = (0, 0),
    ) -> tuple[dict[str, list[dict]], int, int]:
        all_rows: dict[str, list[dict]] = defaultdict(list)
        success_lakes = 0
        error_lakes = 0
        chunk_start, chunk_end = error_chunk

        for idx in range(len(dataset)):
            task = dataset.to_task(idx)
            try:
                result = self._compute_lake(task)
                for table, rows in self.result_to_rows(result).items():
                    all_rows[table].extend(rows)
                success_lakes += 1
            except Exception as exc:
                for table, rows in self.error_to_rows(
                    task.hylak_id,
                    exc,
                    chunk_start,
                    chunk_end,
                ).items():
                    all_rows[table].extend(rows)
                error_lakes += 1
        return dict(all_rows), success_lakes, error_lakes

    def result_to_rows(
        self, result: RunHawkesPipelineResult
    ) -> dict[str, list[dict]]:
        error_msg = result.summary.get("error_message")
        success = error_msg is None
        return {
            f"{_TABLE_PREFIX}_results": [build_hawkes_result_row(result.summary)],
            f"{_TABLE_PREFIX}_lrt": result.lrt_rows,
            f"{_TABLE_PREFIX}_transition_monthly": result.transition_monthly_rows,
            f"{_TABLE_PREFIX}_run_status": [
                make_hawkes_run_status_row(
                    hylak_id=result.summary["hylak_id"],
                    status=RUN_STATUS_DONE if success else RUN_STATUS_ERROR,
                    error_message=error_msg,
                )
            ],
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        error_summary = build_error_summary(hylak_id, str(error), 0.0)
        return {
            f"{_TABLE_PREFIX}_run_status": [
                make_hawkes_run_status_row(
                    hylak_id=hylak_id,
                    status=RUN_STATUS_ERROR,
                    error_message=str(error)[:500],
                )
            ],
            f"{_TABLE_PREFIX}_results": [
                build_hawkes_result_row(error_summary)
            ],
        }
