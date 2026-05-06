"""Similarity metrics for paired lake water_area time series: Pearson and ACF cosine."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from scipy.stats import pearsonr
from statsmodels.tsa.stattools import acf

log = logging.getLogger(__name__)

ACF_PERIOD_MONTHS = 12


def align_series(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    """Align two lake area DataFrames by (year, month) and return water_area arrays.

    Args:
        df_a: DataFrame with columns year, month, water_area.
        df_b: Same schema.

    Returns:
        (arr_a, arr_b) of aligned water_area values; empty arrays if no common index.
    """
    if df_a.empty or df_b.empty:
        return np.array([]), np.array([])
    merged = df_a.merge(
        df_b,
        on=["year", "month"],
        how="inner",
        suffixes=("_a", "_b"),
    )
    if merged.empty:
        return np.array([]), np.array([])
    arr_a = merged["water_area_a"].to_numpy(dtype=float)
    arr_b = merged["water_area_b"].to_numpy(dtype=float)
    return arr_a, arr_b


def pearson_correlation(series_a: np.ndarray, series_b: np.ndarray) -> float:
    """Pearson correlation between two aligned water_area series.

    Args:
        series_a: First water_area array.
        series_b: Second water_area array (same length as series_a).

    Returns:
        Pearson r, or nan if length < 2 or constant series.
    """
    if series_a.size < 2 or series_b.size < 2:
        return np.nan
    if series_a.size != series_b.size:
        return np.nan
    if np.std(series_a) == 0 or np.std(series_b) == 0:
        return np.nan
    r_val, _ = pearsonr(series_a, series_b)
    return float(r_val)


def acf_cosine_similarity(
    series_a: np.ndarray,
    series_b: np.ndarray,
    period: int = ACF_PERIOD_MONTHS,
) -> float:
    """Cosine similarity between ACF vectors at 12-month delay (lags 1..12).

    ACF is computed at lags 1..period (12 months delay), then the two ACF vectors
    are compared by cosine similarity.

    Args:
        series_a: First water_area array.
        series_b: Second water_area array (same length as series_a).
        period: Number of delay lags (default 12 months).

    Returns:
        Cosine similarity in [-1, 1], or nan if insufficient data or zero norm.
    """
    if series_a.size != series_b.size or series_a.size <= period:
        return np.nan
    try:
        acf_a = acf(series_a, nlags=period, fft=True)
        acf_b = acf(series_b, nlags=period, fft=True)
    except (ValueError, np.linalg.LinAlgError):
        return np.nan
    vec_a = np.asarray(acf_a[1 : period + 1], dtype=float)
    vec_b = np.asarray(acf_b[1 : period + 1], dtype=float)
    norm_a = np.linalg.norm(vec_a)
    norm_b = np.linalg.norm(vec_b)
    if norm_a == 0 or norm_b == 0:
        return np.nan
    cos_sim = float(np.dot(vec_a, vec_b) / (norm_a * norm_b))
    return max(-1.0, min(1.0, cos_sim))


def compute_pair_similarity(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
) -> dict:
    """Compute Pearson and ACF cosine similarity for a pair of lake area series.

    Args:
        df_a: DataFrame with columns year, month, water_area for first lake.
        df_b: Same for second lake.

    Returns:
        Dict with keys: pearson_r, acf_cos_sim, n_common.
    """
    arr_a, arr_b = align_series(df_a, df_b)
    n_common = len(arr_a)
    if n_common < 2:
        return {
            "pearson_r": np.nan,
            "acf_cos_sim": np.nan,
            "n_common": n_common,
        }
    pearson_r = pearson_correlation(arr_a, arr_b)
    acf_cos_sim = acf_cosine_similarity(arr_a, arr_b, period=ACF_PERIOD_MONTHS)
    return {
        "pearson_r": pearson_r,
        "acf_cos_sim": acf_cos_sim,
        "n_common": n_common,
    }
