"""Volatility metrics for lake water_area time series."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def compute_cv(series: np.ndarray) -> float:
    """Coefficient of variation (std / mean).

    Returns nan if mean is zero or series has fewer than 2 values.
    """
    if series.size < 2:
        return np.nan
    mean = float(np.mean(series))
    if mean == 0:
        return np.nan
    return float(np.std(series) / mean)


def compute_pct_change_std(series: np.ndarray) -> float:
    """Standard deviation of month-over-month percent changes.

    Returns nan if fewer than 2 percent-change values.
    """
    if series.size < 2:
        return np.nan
    pct = np.diff(series) / np.where(series[:-1] == 0, np.nan, series[:-1])
    pct = pct[~np.isnan(pct)]
    if pct.size < 2:
        return np.nan
    return float(np.std(pct))


def compute_range_ratio(series: np.ndarray) -> float:
    """Range ratio: (max - min) / mean.

    Returns nan if mean is zero or series has fewer than 2 values.
    """
    if series.size < 2:
        return np.nan
    mean = float(np.mean(series))
    if mean == 0:
        return np.nan
    return float((np.max(series) - np.min(series)) / mean)


def compute_lake_metrics(df: pd.DataFrame) -> dict:
    """Compute all volatility metrics for a single lake.

    Args:
        df: DataFrame with columns [year, month, water_area].

    Returns:
        Dict with keys: cv, pct_change_std, range_ratio, n_obs.
    """
    if df.empty:
        return {
            "cv": np.nan,
            "pct_change_std": np.nan,
            "range_ratio": np.nan,
            "n_obs": 0,
        }
    series = df["water_area"].to_numpy(dtype=float)
    return {
        "cv": compute_cv(series),
        "pct_change_std": compute_pct_change_std(series),
        "range_ratio": compute_range_ratio(series),
        "n_obs": len(series),
    }


def compute_pair_metrics(
    df_artificial: pd.DataFrame,
    df_natural: pd.DataFrame,
) -> dict:
    """Compute volatility metrics for an artificial-natural lake pair and their deltas.

    Args:
        df_artificial: DataFrame with columns [year, month, water_area] for the
            artificial lake.
        df_natural: Same for the paired natural lake.

    Returns:
        Dict with keys:
          - cv_a, pct_change_std_a, range_ratio_a, n_obs_a  (artificial)
          - cv_n, pct_change_std_n, range_ratio_n, n_obs_n  (natural)
          - delta_cv, delta_pct_change_std, delta_range_ratio
    """
    m_a = compute_lake_metrics(df_artificial)
    m_n = compute_lake_metrics(df_natural)

    def _delta(a: float, n: float) -> float:
        if np.isnan(a) or np.isnan(n):
            return np.nan
        return a - n

    return {
        "cv_a": m_a["cv"],
        "pct_change_std_a": m_a["pct_change_std"],
        "range_ratio_a": m_a["range_ratio"],
        "n_obs_a": m_a["n_obs"],
        "cv_n": m_n["cv"],
        "pct_change_std_n": m_n["pct_change_std"],
        "range_ratio_n": m_n["range_ratio"],
        "n_obs_n": m_n["n_obs"],
        "delta_cv": _delta(m_a["cv"], m_n["cv"]),
        "delta_pct_change_std": _delta(m_a["pct_change_std"], m_n["pct_change_std"]),
        "delta_range_ratio": _delta(m_a["range_ratio"], m_n["range_ratio"]),
    }
