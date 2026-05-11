"""Hawkes batch output mining: transition lake selection and aggregate statistics.

Extracted from ``scripts/run_hawkes_mining.py``. Pure-data functions with no
CLI or plotting dependencies.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


def load_summary(path: str | Path) -> pd.DataFrame:
    """Load Hawkes batch summary CSV and validate required columns.

    Args:
        path: Path to summary.csv from a Hawkes batch run.

    Returns:
        DataFrame with validated schema.

    Raises:
        FileNotFoundError: If path does not exist.
        ValueError: If required columns are missing.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Summary CSV not found: {path}")
    frame = pd.read_csv(path)
    required = {
        "hylak_id",
        "threshold_quantile",
        "converged",
        "n_events",
        "log_likelihood",
        "spectral_radius",
        "alpha_DW",
        "alpha_WD",
        "beta_DW",
        "beta_WD",
        "lrt_p_D_to_W",
        "lrt_p_W_to_D",
        "error_message",
    }
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"Missing required columns in summary: {sorted(missing)}")
    return frame


def safe_series_divide(numer: pd.Series, denom: pd.Series) -> pd.Series:
    """Element-wise division guarding against zeros and NaNs.

    Inf/-inf results are replaced with NaN.
    """
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce")
    with np.errstate(divide="ignore", invalid="ignore"):
        result = numer / denom
    result = result.replace([np.inf, -np.inf], np.nan)
    return result


def select_transition_lakes(
    summary: pd.DataFrame,
    *,
    p_threshold: float = 0.05,
    alpha_min: float = 1e-3,
    min_events: int = 12,
    quarter_window_years: float = 0.25,
    quarterly_min_mass: float = 0.50,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Select short-memory transition lakes from a Hawkes batch summary.

    A transition is "short-memory" when the excitation mass within a
    quarter-window exceeds ``quarterly_min_mass``, meaning the Hawkes
    kernel decays fast enough that most triggering is concentrated in
    a short period.

    Args:
        summary: Batch summary DataFrame from ``load_summary``.
        p_threshold: LRT p-value significance threshold.
        alpha_min: Minimum cross-alpha magnitude.
        min_events: Minimum event count.
        quarter_window_years: Window length for mass computation.
        quarterly_min_mass: Minimum excitation mass in quarter window.

    Returns:
        (d_to_w, w_to_d, union) DataFrames of transition lakes.
    """
    fit_ok = summary[
        summary["converged"].astype(bool)
        & summary["error_message"].isna()
        & (pd.to_numeric(summary["n_events"], errors="coerce") >= min_events)
    ].copy()
    fit_ok["ll_per_event"] = safe_series_divide(
        fit_ok["log_likelihood"], fit_ok["n_events"]
    )
    fit_ok["mass_q_D_to_W"] = 1.0 - np.exp(
        -quarter_window_years * pd.to_numeric(fit_ok["beta_WD"], errors="coerce")
    )
    fit_ok["mass_q_W_to_D"] = 1.0 - np.exp(
        -quarter_window_years * pd.to_numeric(fit_ok["beta_DW"], errors="coerce")
    )

    d_to_w = fit_ok[
        (pd.to_numeric(fit_ok["lrt_p_D_to_W"], errors="coerce") < p_threshold)
        & (pd.to_numeric(fit_ok["alpha_WD"], errors="coerce") >= alpha_min)
        & (pd.to_numeric(fit_ok["mass_q_D_to_W"], errors="coerce") >= quarterly_min_mass)
    ].copy()
    d_to_w["transition_direction"] = "D_to_W"

    w_to_d = fit_ok[
        (pd.to_numeric(fit_ok["lrt_p_W_to_D"], errors="coerce") < p_threshold)
        & (pd.to_numeric(fit_ok["alpha_DW"], errors="coerce") >= alpha_min)
        & (pd.to_numeric(fit_ok["mass_q_W_to_D"], errors="coerce") >= quarterly_min_mass)
    ].copy()
    w_to_d["transition_direction"] = "W_to_D"

    union = (
        pd.concat([d_to_w, w_to_d], ignore_index=True)
        .sort_values(["hylak_id", "threshold_quantile", "transition_direction"])
        .reset_index(drop=True)
    )
    return d_to_w, w_to_d, union


def build_overall_stats(summary: pd.DataFrame) -> dict:
    """Compute aggregate fit-quality statistics from batch summary.

    Args:
        summary: Batch summary DataFrame.

    Returns:
        Dict with convergence/event/spectral-radius stats.
    """
    total = len(summary)
    converged = int(summary["converged"].fillna(False).astype(bool).sum())
    failed = int(summary["error_message"].notna().sum())
    ok = summary[
        summary["converged"].fillna(False).astype(bool)
        & summary["error_message"].isna()
    ].copy()
    ok["ll_per_event"] = safe_series_divide(ok["log_likelihood"], ok["n_events"])
    stats = {
        "n_total_rows": int(total),
        "n_converged": converged,
        "n_failed": failed,
        "convergence_rate": float(converged / total) if total > 0 else float("nan"),
        "failure_rate": float(failed / total) if total > 0 else float("nan"),
        "median_n_events": float(
            pd.to_numeric(ok["n_events"], errors="coerce").median()
        ),
        "p25_n_events": float(
            pd.to_numeric(ok["n_events"], errors="coerce").quantile(0.25)
        ),
        "p75_n_events": float(
            pd.to_numeric(ok["n_events"], errors="coerce").quantile(0.75)
        ),
        "median_ll_per_event": float(
            pd.to_numeric(ok["ll_per_event"], errors="coerce").median()
        ),
        "median_spectral_radius": float(
            pd.to_numeric(ok["spectral_radius"], errors="coerce").median()
        ),
        "stable_ratio_rho_lt_1": float(
            (pd.to_numeric(ok["spectral_radius"], errors="coerce") < 1.0).mean()
        ),
    }
    return stats


def load_events_from_case(case_output_dir: str | Path) -> pd.DataFrame:
    """Load per-case events.csv and convert to EOT-style columns.

    Args:
        case_output_dir: Directory containing events.csv from a Hawkes batch case.

    Returns:
        DataFrame with columns [tail, year, month, water_area, threshold_at_event].
        Returns empty DataFrame with correct columns if events.csv is missing or empty.
    """
    events_path = Path(case_output_dir) / "events.csv"
    empty_result = pd.DataFrame(
        columns=["tail", "year", "month", "water_area", "threshold_at_event"]
    )
    if not events_path.exists():
        return empty_result
    events = pd.read_csv(events_path)
    if events.empty:
        return empty_result
    converted = pd.DataFrame({
        "tail": np.where(events["event_label"] == "W", "high", "low"),
        "year": pd.to_numeric(events["year"], errors="coerce").astype("Int64"),
        "month": pd.to_numeric(events["month"], errors="coerce").astype("Int64"),
        "water_area": pd.to_numeric(events["original_value"], errors="coerce"),
        "threshold_at_event": pd.to_numeric(events["threshold"], errors="coerce"),
    })
    return converted.dropna(
        subset=["year", "month", "water_area", "threshold_at_event"]
    ).copy()
