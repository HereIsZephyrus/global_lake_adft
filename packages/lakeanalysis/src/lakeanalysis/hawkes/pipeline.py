"""Hawkes pipeline: QC, fitting, LRT, decomposition, and result assembly.

Orchestrates the complete Hawkes fitting pipeline from an event series through
to summary stats, LRT rows, and monthly transition decomposition rows.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import numpy as np
import pandas as pd

from .fit import (
    LikelihoodRatioTest,
    fit_full_model,
    fit_restricted_model,
    run_model_comparison,
)
from .model import evaluate_intensity_decomposition
from .types import HawkesEventSeries, TYPE_DRY, TYPE_WET


class HawkesQCFailError(Exception):
    """QC check failed before Hawkes fitting."""
    def __init__(self, message: str, qc: dict) -> None:
        super().__init__(message)
        self.qc = qc


@dataclass(frozen=True)
class RunHawkesPipelineResult:
    """Complete output of one lake through the Hawkes pipeline."""
    summary: dict
    lrt_rows: list[dict]
    transition_monthly_rows: list[dict]


def quantile_string(value: float) -> str:
    """Return deterministic decimal string for quantile keys."""
    return str(Decimal(str(value)))


def compute_qc_metrics(
    series_df: pd.DataFrame,
    event_series: HawkesEventSeries,
    events_table: pd.DataFrame,
    min_event_rate: float,
    max_event_rate: float,
    min_relative_amplitude: float,
    min_median_severity: float,
) -> tuple[dict, bool]:
    """Compute and evaluate quality-control metrics for Hawkes entry.

    Uses ``severity`` column when available (PWM events), otherwise falls back
    to ``value - threshold`` (EOT extremes).
    """
    n_obs = len(series_df)
    n_events = len(event_series.times)
    event_rate = float(n_events / n_obs) if n_obs > 0 else float("nan")

    values = (
        pd.to_numeric(series_df["water_area"], errors="coerce")
        .dropna()
        .to_numpy(dtype=float)
    )
    if values.size > 0:
        p95 = float(np.quantile(values, 0.95))
        p05 = float(np.quantile(values, 0.05))
        median = float(np.median(values))
        relative_amplitude = float((p95 - p05) / max(abs(median), 1.0))
    else:
        relative_amplitude = float("nan")

    if events_table.empty:
        median_severity = float("nan")
    elif "severity" in events_table.columns:
        severity_vals = pd.to_numeric(
            events_table["severity"], errors="coerce"
        ).to_numpy(dtype=float)
        median_severity = (
            float(np.median(severity_vals)) if severity_vals.size > 0 else float("nan")
        )
    else:
        excess = np.abs(
            pd.to_numeric(events_table["value"], errors="coerce").to_numpy(
                dtype=float
            )
            - pd.to_numeric(events_table["threshold"], errors="coerce").to_numpy(
                dtype=float
            )
        )
        median_severity = (
            float(np.median(excess)) if excess.size > 0 else float("nan")
        )

    qc: dict = {
        "qc_event_rate": event_rate,
        "qc_relative_amplitude": relative_amplitude,
        "qc_median_severity": median_severity,
        "qc_pass_event_rate": bool(
            np.isfinite(event_rate)
            and min_event_rate <= event_rate <= max_event_rate
        ),
        "qc_pass_relative_amplitude": bool(
            np.isfinite(relative_amplitude)
            and relative_amplitude >= min_relative_amplitude
        ),
        "qc_pass_median_severity": bool(
            np.isfinite(median_severity) and median_severity >= min_median_severity
        ),
    }
    qc_pass = bool(
        qc["qc_pass_event_rate"]
        and qc["qc_pass_relative_amplitude"]
        and qc["qc_pass_median_severity"]
    )
    return qc, qc_pass


def build_hawkes_result_row(summary: dict) -> dict:
    """Map a fit-summary dict to a hawkes_results row."""
    return {
        "hylak_id": int(summary["hylak_id"]),
        "threshold_quantile": quantile_string(
            float(summary["threshold_quantile"])
        ),
        "converged": summary.get("converged"),
        "log_likelihood": summary.get("log_likelihood"),
        "objective_value": summary.get("objective_value"),
        "n_events": summary.get("n_events"),
        "n_dry_events": summary.get("n_dry_events"),
        "n_wet_events": summary.get("n_wet_events"),
        "mu_d": summary.get("mu_D"),
        "mu_w": summary.get("mu_W"),
        "alpha_dd": summary.get("alpha_DD"),
        "alpha_dw": summary.get("alpha_DW"),
        "alpha_wd": summary.get("alpha_WD"),
        "alpha_ww": summary.get("alpha_WW"),
        "beta_dd": summary.get("beta_DD"),
        "beta_dw": summary.get("beta_DW"),
        "beta_wd": summary.get("beta_WD"),
        "beta_ww": summary.get("beta_WW"),
        "spectral_radius": summary.get("spectral_radius"),
        "lrt_p_d_to_w": summary.get("lrt_p_D_to_W"),
        "lrt_p_w_to_d": summary.get("lrt_p_W_to_D"),
        "qc_pass": summary.get("qc_pass"),
        "qc_exceedance_rate": summary.get("qc_event_rate"),
        "qc_relative_amplitude": summary.get("qc_relative_amplitude"),
        "qc_median_excess": summary.get("qc_median_severity"),
        "error_message": summary.get("error_message"),
    }


def build_hawkes_transition_monthly_rows(
    hylak_id: int,
    threshold_quantile: float,
    decomposition: pd.DataFrame,
    timeline: pd.DataFrame,
    significance_quantile: float,
) -> list[dict]:
    """Build monthly D->W / W->D significance rows from decomposition."""
    if decomposition.empty or timeline.empty:
        return []
    if not (0.0 < significance_quantile < 1.0):
        raise ValueError("significance_quantile must be in (0, 1)")

    joined = timeline.loc[:, ["year", "month", "time"]].merge(
        decomposition.loc[
            :, ["time", "cross_D", "cross_W", "lambda_D", "lambda_W"]
        ],
        on="time",
        how="inner",
    )
    if joined.empty:
        return []

    joined["score_raw_D_to_W"] = pd.to_numeric(
        joined["cross_W"], errors="coerce"
    )
    joined["score_raw_W_to_D"] = pd.to_numeric(
        joined["cross_D"], errors="coerce"
    )
    joined["score_norm_D_to_W"] = joined["score_raw_D_to_W"] / pd.to_numeric(
        joined["lambda_W"], errors="coerce"
    ).replace(0.0, np.nan)
    joined["score_norm_W_to_D"] = joined["score_raw_W_to_D"] / pd.to_numeric(
        joined["lambda_D"], errors="coerce"
    ).replace(0.0, np.nan)

    rows: list[dict] = []
    q_str = quantile_string(threshold_quantile)
    for direction, score_raw_col, score_norm_col in (
        ("D_to_W", "score_raw_D_to_W", "score_norm_D_to_W"),
        ("W_to_D", "score_raw_W_to_D", "score_norm_W_to_D"),
    ):
        valid_norm = joined[score_norm_col].dropna().to_numpy(dtype=float)
        threshold = (
            float(np.quantile(valid_norm, significance_quantile))
            if valid_norm.size > 0
            else float("nan")
        )
        for _, row in joined.iterrows():
            score_raw = row[score_raw_col]
            score_norm = row[score_norm_col]
            is_significant = bool(
                np.isfinite(score_norm)
                and np.isfinite(threshold)
                and float(score_norm) >= threshold
            )
            rows.append(
                {
                    "hylak_id": int(hylak_id),
                    "threshold_quantile": q_str,
                    "year": int(row["year"]),
                    "month": int(row["month"]),
                    "direction": direction,
                    "score_raw": (
                        float(score_raw) if np.isfinite(score_raw) else None
                    ),
                    "score_norm": (
                        float(score_norm) if np.isfinite(score_norm) else None
                    ),
                    "significance_quantile": float(significance_quantile),
                    "significance_threshold": (
                        float(threshold) if np.isfinite(threshold) else None
                    ),
                    "significant": is_significant,
                }
            )
    return rows


def build_error_summary(
    hylak_id: int,
    message: str,
    threshold_quantile: float = 0.0,
) -> dict:
    """Build a summary dict for error / failed runs."""
    return {
        "hylak_id": int(hylak_id),
        "threshold_quantile": float(threshold_quantile),
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


def build_qc_fail_summary(
    hylak_id: int,
    qc: dict,
    message: str,
    threshold_quantile: float = 0.0,
) -> dict:
    """Build a summary dict for QC-fail runs, including QC metric values."""
    summary = build_error_summary(hylak_id, message, threshold_quantile)
    summary["qc_pass"] = False
    summary["qc_event_rate"] = qc.get("qc_event_rate")
    summary["qc_relative_amplitude"] = qc.get("qc_relative_amplitude")
    summary["qc_median_severity"] = qc.get("qc_median_severity")
    return summary


def make_hawkes_run_status_row(
    *,
    hylak_id: int,
    status: str,
    error_message: str | None = None,
) -> dict:
    """Build a hawkes run_status row shared by EOT and PWM workflows."""
    valid_statuses = {"done", "error"}
    if status not in valid_statuses:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": int(hylak_id),
        "chunk_start": 0,
        "chunk_end": 0,
        "status": status,
        "error_message": error_message,
    }


def run_hawkes_pipeline(
    event_series: HawkesEventSeries,
    events_table: pd.DataFrame,
    series_df: pd.DataFrame,
    *,
    hylak_id: int,
    threshold_quantile: float = 0.0,
    hawkes_window_months: float = 4.0,
    min_event_rate: float = 0.01,
    max_event_rate: float = 0.30,
    min_relative_amplitude: float = 0.05,
    min_median_severity: float = 1.0,
    monthly_significance_quantile: float = 0.95,
) -> RunHawkesPipelineResult:
    """Orchestrate the full Hawkes pipeline for one lake.

    Pipeline steps:
        1. QC check
        2. Fit full bivariate Hawkes model
        3. Fit restricted models (disable D→W and W→D edges)
        4. Likelihood ratio tests for cross-excitation significance
        5. Intensity decomposition for monthly transition scores
        6. Assemble summary, LRT rows, and monthly rows

    Raises:
        HawkesQCFailError: When QC checks fail before fitting.
    """
    qc, qc_pass = compute_qc_metrics(
        series_df=series_df,
        event_series=event_series,
        events_table=events_table,
        min_event_rate=min_event_rate,
        max_event_rate=max_event_rate,
        min_relative_amplitude=min_relative_amplitude,
        min_median_severity=min_median_severity,
    )
    if not qc_pass:
        msg = (
            "QC failed before Hawkes fit: "
            f"rate={qc['qc_event_rate']:.4f}, "
            f"rel_amp={qc['qc_relative_amplitude']:.6f}, "
            f"median_severity={qc['qc_median_severity']:.6f}"
        )
        raise HawkesQCFailError(msg, qc)

    full_fit = fit_full_model(
        event_series,
        window_months=hawkes_window_months,
    )
    restricted_d_to_w = fit_restricted_model(
        event_series=event_series,
        disabled_edges=[(TYPE_WET, TYPE_DRY)],
        window_months=hawkes_window_months,
    )
    restricted_w_to_d = fit_restricted_model(
        event_series=event_series,
        disabled_edges=[(TYPE_DRY, TYPE_WET)],
        window_months=hawkes_window_months,
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
            [event_series.start_time, event_series.end_time], dtype=float
        )

    decomposition = evaluate_intensity_decomposition(
        event_series=event_series,
        fit_result=full_fit,
        evaluation_times=evaluation_times,
        window_years=hawkes_window_months / 12.0,
    )

    q_str = quantile_string(threshold_quantile)
    lrt_frame = pd.DataFrame([
        {
            "hylak_id": int(hylak_id),
            "threshold_quantile": q_str,
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
            "threshold_quantile": q_str,
            "test_name": lrt_w_to_d.test_name,
            "lr_statistic": lrt_w_to_d.lr_statistic,
            "df": lrt_w_to_d.df,
            "p_value": lrt_w_to_d.p_value,
            "significance_level": lrt_w_to_d.significance_level,
            "reject_null": lrt_w_to_d.reject_null,
            "restricted_log_likelihood": lrt_w_to_d.restricted_log_likelihood,
            "full_log_likelihood": lrt_w_to_d.full_log_likelihood,
        },
    ])

    summary = {
        "hylak_id": int(hylak_id),
        "threshold_quantile": float(threshold_quantile),
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
        threshold_quantile=threshold_quantile,
        decomposition=decomposition,
        timeline=(
            event_series.timeline
            if event_series.timeline is not None
            else pd.DataFrame()
        ),
        significance_quantile=monthly_significance_quantile,
    )

    return RunHawkesPipelineResult(
        summary=summary,
        lrt_rows=lrt_frame.to_dict(orient="records"),
        transition_monthly_rows=monthly_rows,
    )
