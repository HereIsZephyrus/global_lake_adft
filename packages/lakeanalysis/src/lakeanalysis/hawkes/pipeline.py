"""Reusable Hawkes pipeline helpers shared by EOT and PWM batch workflows."""

from __future__ import annotations

from decimal import Decimal

import numpy as np
import pandas as pd

from .types import HawkesEventSeries


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
