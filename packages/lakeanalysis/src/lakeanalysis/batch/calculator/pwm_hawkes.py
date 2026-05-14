"""PWMExtremeHawkesCalculator: PWM event -> exponential decay C_k -> Hawkes fit.

Uses ``run_single_lake_service`` (STL decomposition + pooled PWM) so that
the event-detection math is identical to the standalone PWM batch pipeline.

Replaces the old hard-threshold runs declustering with an exponential
decay index C_k and transition/unilateral segment extraction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from lakeanalysis.hawkes import (
    HawkesQCFailError,
    RunHawkesPipelineResult,
    build_error_summary,
    build_hawkes_result_row,
    build_qc_fail_summary,
    build_events_from_pwm,
    make_hawkes_run_status_row,
    run_hawkes_pipeline,
)
from lakeanalysis.pwm_extreme.events import (
    compute_decay_index,
    extract_hawkes_events_from_segments,
    extract_segments,
)
from lakeanalysis.pwm_extreme.service import run_single_lake_service
from lakesource.pwm_extreme.schema import (
    PWMExtremeConfig,
    PWMExtremeServiceConfig,
)

from ..domain import Calculator, LakeTask

log = logging.getLogger(__name__)

RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"

_TABLE_PREFIX = "pwm_hawkes"


@dataclass(frozen=True)
class PWMHawkesPipelineResult:
    """Thin wrapper adding segments_rows to the shared pipeline result."""
    pipeline: RunHawkesPipelineResult
    segments_rows: list[dict]


class PWMExtremeHawkesCalculator(Calculator):
    """Batch calculator: PWM extreme events + decay index → Hawkes fitting."""

    def __init__(
        self,
        *,
        pwm_config: PWMExtremeConfig | None = None,
        decay_rate: float = 1.0,
        hawkes_window_months: float = 4.0,
        min_events: int = 10,
        min_event_rate: float = 0.01,
        max_event_rate: float = 0.30,
        min_relative_amplitude: float = 0.05,
        min_median_severity: float = 1.0,
        monthly_significance_quantile: float = 0.95,
        method: str = "stl",
    ) -> None:
        self._pwm_config = pwm_config or PWMExtremeConfig()
        self._service_config = PWMExtremeServiceConfig(
            pwm_config=self._pwm_config,
            method=method,
        )
        self._decay_rate = decay_rate
        self._hawkes_window_months = hawkes_window_months
        self._min_events = min_events
        self._min_event_rate = min_event_rate
        self._max_event_rate = max_event_rate
        self._min_relative_amplitude = min_relative_amplitude
        self._min_median_severity = min_median_severity
        self._monthly_significance_quantile = monthly_significance_quantile

    def run(self, task: LakeTask) -> PWMHawkesPipelineResult:
        hylak_id = task.hylak_id
        series_df = task.series_df
        frozen = set(task.frozen_year_months) if task.frozen_year_months else set()

        try:
            pwm_result = run_single_lake_service(
                series_df,
                hylak_id=hylak_id,
                config=self._service_config,
                frozen_year_months=frozen or None,
            )

            decay_df = compute_decay_index(
                pwm_result.labels_df,
                decay_rate=self._decay_rate,
            )
            segments_df = extract_segments(decay_df)
            segments_rows = _build_segments_rows(hylak_id, segments_df)

            events_df = extract_hawkes_events_from_segments(
                pwm_result.labels_df, decay_df, segments_df
            )

            if len(events_df) < self._min_events:
                return _make_fail_result(
                    hylak_id,
                    f"only {len(events_df)} segment-scoped events < min {self._min_events}",
                )

            event_series, events_table = build_events_from_pwm(
                events_df, series_df
            )

            pipeline_result = run_hawkes_pipeline(
                event_series,
                events_table,
                series_df,
                hylak_id=hylak_id,
                threshold_quantile=0.0,
                hawkes_window_months=self._hawkes_window_months,
                min_event_rate=self._min_event_rate,
                max_event_rate=self._max_event_rate,
                min_relative_amplitude=self._min_relative_amplitude,
                min_median_severity=self._min_median_severity,
                monthly_significance_quantile=self._monthly_significance_quantile,
            )
            return PWMHawkesPipelineResult(
                pipeline=pipeline_result, segments_rows=segments_rows
            )
        except HawkesQCFailError as e:
            summary = build_qc_fail_summary(hylak_id, e.qc, str(e))
            return PWMHawkesPipelineResult(
                pipeline=RunHawkesPipelineResult(
                    summary=summary, lrt_rows=[], transition_monthly_rows=[]
                ),
                segments_rows=[],
            )
        except Exception as exc:
            log.debug("PWM-Hawkes failed for hylak_id=%d: %s", task.hylak_id, exc)
            error_summary = build_error_summary(hylak_id, str(exc))
            return PWMHawkesPipelineResult(
                pipeline=RunHawkesPipelineResult(
                    summary=error_summary, lrt_rows=[], transition_monthly_rows=[]
                ),
                segments_rows=[],
            )

    def result_to_rows(
        self, result: PWMHawkesPipelineResult
    ) -> dict[str, list[dict]]:
        pr = result.pipeline
        error_msg = pr.summary.get("error_message")
        success = error_msg is None
        return {
            f"{_TABLE_PREFIX}_results": [build_hawkes_result_row(pr.summary)],
            f"{_TABLE_PREFIX}_lrt": pr.lrt_rows,
            f"{_TABLE_PREFIX}_transition_monthly": pr.transition_monthly_rows,
            f"{_TABLE_PREFIX}_segments": result.segments_rows,
            f"{_TABLE_PREFIX}_run_status": [
                make_hawkes_run_status_row(
                    hylak_id=pr.summary["hylak_id"],
                    status=RUN_STATUS_DONE if success else RUN_STATUS_ERROR,
                    error_message=error_msg,
                )
            ],
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        error_summary = build_error_summary(hylak_id, str(error))
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


def _make_fail_result(hylak_id: int, message: str) -> PWMHawkesPipelineResult:
    summary = build_error_summary(hylak_id, message)
    summary["qc_pass"] = False
    return PWMHawkesPipelineResult(
        pipeline=RunHawkesPipelineResult(
            summary=summary, lrt_rows=[], transition_monthly_rows=[]
        ),
        segments_rows=[],
    )


def _build_segments_rows(hylak_id: int, segments_df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    if segments_df.empty:
        return rows
    for _, seg in segments_df.iterrows():
        rows.append(
            {
                "hylak_id": hylak_id,
                "segment_id": int(seg["segment_id"]),
                "start_year": int(seg["start_year"]),
                "start_month": int(seg["start_month"]),
                "end_year": int(seg["end_year"]),
                "end_month": int(seg["end_month"]),
                "duration_months": int(seg["duration_months"]),
                "segment_type": str(seg["segment_type"]),
                "has_high": bool(seg["has_high"]),
                "has_low": bool(seg["has_low"]),
                "max_C": float(seg["max_C"]),
                "mean_C": float(seg["mean_C"]),
                "integral_C": float(seg["integral_C"]),
                "n_extreme_events": int(seg["n_extreme_events"]),
                "first_extreme_type": (
                    str(seg["first_extreme_type"])
                    if seg.get("first_extreme_type") is not None
                    else None
                ),
                "last_extreme_type": (
                    str(seg["last_extreme_type"])
                    if seg.get("last_extreme_type") is not None
                    else None
                ),
                "workflow_version": None,
            }
        )
    return rows
