"""PWMExtremeHawkesCalculator: PWM event -> exponential decay C_k -> Hawkes fit.

Uses ``run_single_lake_service`` (STL decomposition + pooled PWM) so that
the event-detection math is identical to the standalone PWM batch pipeline.

Replaces the old hard-threshold runs declustering with an exponential
decay index C_k and transition/unilateral segment extraction.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from lakeanalysis.hawkes import (
    TYPE_DRY,
    TYPE_WET,
    fit_full_model,
    fit_restricted_model,
    run_model_comparison,
    evaluate_intensity_decomposition,
    LikelihoodRatioTest,
)
from lakeanalysis.hawkes.pipeline import (
    build_hawkes_result_row,
    build_hawkes_transition_monthly_rows,
    compute_qc_metrics,
    quantile_string,
)
from lakeanalysis.pwm_extreme.events import (
    build_hawkes_event_series_from_pwm_events,
    compute_decay_index,
    extract_segments,
)
from lakeanalysis.pwm_extreme.service import run_single_lake_service
from lakesource.pwm_extreme.schema import (
    PWMExtremeConfig,
    PWMExtremeServiceConfig,
)

from ..engine import Calculator, LakeTask

log = logging.getLogger(__name__)

RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"


@dataclass(frozen=True)
class PWMHawkesFitResult:
    hylak_id: int
    success: bool
    summary: dict | None
    lrt_rows: list[dict]
    hawkes_result_rows: list[dict]
    transition_monthly_rows: list[dict]
    segments_rows: list[dict]
    error_message: str | None


class PWMExtremeHawkesCalculator(Calculator):
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

    def run(self, task: LakeTask) -> PWMHawkesFitResult:
        try:
            hylak_id = task.hylak_id
            series_df = task.series_df
            frozen = set(task.frozen_year_months) if task.frozen_year_months else set()

            pwm_result = run_single_lake_service(
                series_df,
                hylak_id=hylak_id,
                config=self._service_config,
                frozen_year_months=frozen or None,
            )
            raw_events = pwm_result.extremes_df

            if len(raw_events) < self._min_events:
                return self._fail_qc(
                    hylak_id,
                    f"only {len(raw_events)} raw extreme months < min {self._min_events}",
                )

            raw_events = raw_events.copy()
            raw_events["time"] = (
                raw_events["year"] + (raw_events["month"] - 1) / 12.0
            )

            event_series, events_table = build_hawkes_event_series_from_pwm_events(
                raw_events, series_df
            )

            decay_df = compute_decay_index(
                pwm_result.labels_df,
                decay_rate=self._decay_rate,
            )
            segments_df = extract_segments(decay_df)
            segments_rows = _build_segments_rows(hylak_id, segments_df)

            qc, qc_pass = compute_qc_metrics(
                series_df=series_df,
                event_series=event_series,
                events_table=events_table,
                min_event_rate=self._min_event_rate,
                max_event_rate=self._max_event_rate,
                min_relative_amplitude=self._min_relative_amplitude,
                min_median_severity=self._min_median_severity,
            )
            if not qc_pass:
                msg = (
                    "QC failed before Hawkes fit: "
                    f"rate={qc['qc_event_rate']:.4f}, "
                    f"rel_amp={qc['qc_relative_amplitude']:.6f}, "
                    f"median_severity={qc['qc_median_severity']:.6f}"
                )
                summary = self._build_qc_fail_summary(hylak_id, qc, msg)
                return PWMHawkesFitResult(
                    hylak_id=hylak_id,
                    success=False,
                    summary=summary,
                    lrt_rows=[],
                    hawkes_result_rows=[build_hawkes_result_row(summary)],
                    transition_monthly_rows=[],
                    segments_rows=segments_rows,
                    error_message=msg,
                )

            full_fit = fit_full_model(
                event_series,
                window_months=self._hawkes_window_months,
            )
            restricted_d_to_w = fit_restricted_model(
                event_series=event_series,
                disabled_edges=[(TYPE_WET, TYPE_DRY)],
                window_months=self._hawkes_window_months,
            )
            restricted_w_to_d = fit_restricted_model(
                event_series=event_series,
                disabled_edges=[(TYPE_DRY, TYPE_WET)],
                window_months=self._hawkes_window_months,
            )
            strategy = LikelihoodRatioTest(significance_level=0.05)
            lrt_d_to_w = run_model_comparison(
                test_name="D_to_W",
                restricted_fit=restricted_d_to_w,
                full_fit=full_fit,
                df=1,
                test_strategy=strategy,
            )
            lrt_w_to_d = run_model_comparison(
                test_name="W_to_D",
                restricted_fit=restricted_w_to_d,
                full_fit=full_fit,
                df=1,
                test_strategy=strategy,
            )

            if event_series.timeline is not None and not event_series.timeline.empty:
                evaluation_times = event_series.timeline["time"].to_numpy(dtype=float)
            else:
                evaluation_times = np.array(
                    [event_series.start_time, event_series.end_time],
                    dtype=float,
                )
            decomposition = evaluate_intensity_decomposition(
                event_series=event_series,
                fit_result=full_fit,
                evaluation_times=evaluation_times,
                window_years=self._hawkes_window_months / 12.0,
            )

            lrt_frame = pd.DataFrame(
                [
                    {
                        "hylak_id": int(hylak_id),
                        "threshold_quantile": quantile_string(0.0),
                        "test_name": lrt_d_to_w.test_name,
                        "lr_statistic": lrt_d_to_w.lr_statistic,
                        "df": lrt_d_to_w.df,
                        "p_value": lrt_d_to_w.p_value,
                        "significance_level": lrt_d_to_w.significance_level,
                        "reject_null": lrt_d_to_w.reject_null,
                        "restricted_log_likelihood": lrt_d_to_w.restricted_log_likelihood,
                        "full_log_likelihood": lrt_d_to_w.full_log_likelihood,
                    },
                    {
                        "hylak_id": int(hylak_id),
                        "threshold_quantile": quantile_string(0.0),
                        "test_name": lrt_w_to_d.test_name,
                        "lr_statistic": lrt_w_to_d.lr_statistic,
                        "df": lrt_w_to_d.df,
                        "p_value": lrt_w_to_d.p_value,
                        "significance_level": lrt_w_to_d.significance_level,
                        "reject_null": lrt_w_to_d.reject_null,
                        "restricted_log_likelihood": lrt_w_to_d.restricted_log_likelihood,
                        "full_log_likelihood": lrt_w_to_d.full_log_likelihood,
                    },
                ]
            )

            summary = {
                "hylak_id": int(hylak_id),
                "threshold_quantile": 0.0,
                "converged": bool(full_fit.converged),
                "message": full_fit.message,
                "n_events": int(len(event_series.times)),
                "n_dry_events": int((event_series.event_types == TYPE_DRY).sum()),
                "n_wet_events": int((event_series.event_types == TYPE_WET).sum()),
                "log_likelihood": float(full_fit.log_likelihood),
                "objective_value": float(full_fit.objective_value),
                "mu_D": float(full_fit.mu[TYPE_DRY]),
                "mu_W": float(full_fit.mu[TYPE_WET]),
                "alpha_DD": float(full_fit.alpha[TYPE_DRY, TYPE_DRY]),
                "alpha_DW": float(full_fit.alpha[TYPE_DRY, TYPE_WET]),
                "alpha_WD": float(full_fit.alpha[TYPE_WET, TYPE_DRY]),
                "alpha_WW": float(full_fit.alpha[TYPE_WET, TYPE_WET]),
                "beta_DD": float(full_fit.beta[TYPE_DRY, TYPE_DRY]),
                "beta_DW": float(full_fit.beta[TYPE_DRY, TYPE_WET]),
                "beta_WD": float(full_fit.beta[TYPE_WET, TYPE_DRY]),
                "beta_WW": float(full_fit.beta[TYPE_WET, TYPE_WET]),
                "spectral_radius": float(full_fit.spectral_radius),
                "lrt_p_D_to_W": float(lrt_d_to_w.p_value),
                "lrt_p_W_to_D": float(lrt_w_to_d.p_value),
                "qc_pass": True,
                "qc_event_rate": qc["qc_event_rate"],
                "qc_relative_amplitude": qc["qc_relative_amplitude"],
                "qc_median_severity": qc["qc_median_severity"],
                "error_message": None,
            }
            monthly_rows = build_hawkes_transition_monthly_rows(
                hylak_id=hylak_id,
                threshold_quantile=0.0,
                decomposition=decomposition,
                timeline=(
                    event_series.timeline
                    if event_series.timeline is not None
                    else pd.DataFrame()
                ),
                significance_quantile=self._monthly_significance_quantile,
            )
            return PWMHawkesFitResult(
                hylak_id=hylak_id,
                success=True,
                summary=summary,
                lrt_rows=lrt_frame.to_dict(orient="records"),
                hawkes_result_rows=[build_hawkes_result_row(summary)],
                transition_monthly_rows=monthly_rows,
                segments_rows=segments_rows,
                error_message=None,
            )
        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.debug("PWM-Hawkes failed for hylak_id=%d: %s", task.hylak_id, exc)
            error_summary = self._build_error_summary(task.hylak_id, str(exc))
            return PWMHawkesFitResult(
                hylak_id=task.hylak_id,
                success=False,
                summary=error_summary,
                lrt_rows=[],
                hawkes_result_rows=[build_hawkes_result_row(error_summary)],
                transition_monthly_rows=[],
                segments_rows=[],
                error_message=str(exc)[:500],
            )

    def result_to_rows(self, result: PWMHawkesFitResult) -> dict[str, list[dict]]:
        return {
            "pwm_hawkes_results": result.hawkes_result_rows,
            "pwm_hawkes_lrt": result.lrt_rows,
            "pwm_hawkes_transition_monthly": result.transition_monthly_rows,
            "pwm_hawkes_segments": result.segments_rows,
            "pwm_hawkes_run_status": [
                make_pwm_hawkes_run_status_row(
                    hylak_id=result.hylak_id,
                    status=RUN_STATUS_DONE if result.success else RUN_STATUS_ERROR,
                    error_message=result.error_message,
                )
            ],
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        return {
            "pwm_hawkes_run_status": [
                make_pwm_hawkes_run_status_row(
                    hylak_id=hylak_id,
                    status=RUN_STATUS_ERROR,
                    error_message=str(error)[:500],
                )
            ],
            "pwm_hawkes_results": [
                build_hawkes_result_row(self._build_error_summary(hylak_id, str(error)))
            ],
        }

    def _fail_qc(self, hylak_id: int, message: str) -> PWMHawkesFitResult:
        summary = {
            "hylak_id": int(hylak_id),
            "threshold_quantile": 0.0,
            "converged": False,
            "message": message,
            "n_events": 0,
            "n_dry_events": None,
            "n_wet_events": None,
            "log_likelihood": None,
            "objective_value": None,
            "mu_D": None,
            "mu_W": None,
            "alpha_DD": None,
            "alpha_DW": None,
            "alpha_WD": None,
            "alpha_WW": None,
            "beta_DD": None,
            "beta_DW": None,
            "beta_WD": None,
            "beta_WW": None,
            "spectral_radius": None,
            "lrt_p_D_to_W": None,
            "lrt_p_W_to_D": None,
            "qc_pass": False,
            "qc_event_rate": None,
            "qc_relative_amplitude": None,
            "qc_median_severity": None,
            "error_message": message,
        }
        return PWMHawkesFitResult(
            hylak_id=hylak_id,
            success=False,
            summary=summary,
            lrt_rows=[],
            hawkes_result_rows=[build_hawkes_result_row(summary)],
            transition_monthly_rows=[],
            segments_rows=[],
            error_message=message,
        )

    def _build_qc_fail_summary(
        self, hylak_id: int, qc: dict, message: str
    ) -> dict:
        return {
            "hylak_id": int(hylak_id),
            "threshold_quantile": 0.0,
            "converged": False,
            "message": message,
            "n_events": None,
            "n_dry_events": None,
            "n_wet_events": None,
            "log_likelihood": None,
            "objective_value": None,
            "mu_D": None,
            "mu_W": None,
            "alpha_DD": None,
            "alpha_DW": None,
            "alpha_WD": None,
            "alpha_WW": None,
            "beta_DD": None,
            "beta_DW": None,
            "beta_WD": None,
            "beta_WW": None,
            "spectral_radius": None,
            "lrt_p_D_to_W": None,
            "lrt_p_W_to_D": None,
            "qc_pass": False,
            "qc_event_rate": qc.get("qc_event_rate"),
            "qc_relative_amplitude": qc.get("qc_relative_amplitude"),
            "qc_median_severity": qc.get("qc_median_severity"),
            "error_message": message,
        }

    @staticmethod
    def _build_error_summary(hylak_id: int, message: str) -> dict:
        return {
            "hylak_id": int(hylak_id),
            "threshold_quantile": 0.0,
            "converged": False,
            "message": message,
            "n_events": None,
            "n_dry_events": None,
            "n_wet_events": None,
            "log_likelihood": None,
            "objective_value": None,
            "mu_D": None,
            "mu_W": None,
            "alpha_DD": None,
            "alpha_DW": None,
            "alpha_WD": None,
            "alpha_WW": None,
            "beta_DD": None,
            "beta_DW": None,
            "beta_WD": None,
            "beta_WW": None,
            "spectral_radius": None,
            "lrt_p_D_to_W": None,
            "lrt_p_W_to_D": None,
            "qc_pass": None,
            "qc_event_rate": None,
            "qc_relative_amplitude": None,
            "qc_median_severity": None,
            "error_message": message,
        }


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


def make_pwm_hawkes_run_status_row(
    *,
    hylak_id: int,
    status: str,
    error_message: str | None = None,
) -> dict:
    valid_statuses = {RUN_STATUS_DONE, RUN_STATUS_ERROR}
    if status not in valid_statuses:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": int(hylak_id),
        "chunk_start": 0,
        "chunk_end": 0,
        "status": status,
        "error_message": error_message,
    }
