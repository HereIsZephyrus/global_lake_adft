"""Entropy plotting helpers and compatibility wrappers."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from lakeviz.style.presets import Theme
from lakeviz.domain.entropy import (
    plot_ae_distribution as _plot_ae_distribution,
    plot_amplitude_histogram as _plot_amplitude_histogram,
    plot_amplitude_vs_entropy as _plot_amplitude_vs_entropy,
    plot_annual_ae as _plot_annual_ae,
    plot_trend_summary as _plot_trend_summary,
)

log = logging.getLogger(__name__)

AMPLITUDE_OUTLIER_IQR_MULT = 1.5


def remove_amplitude_outliers(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where |mean_seasonal_amplitude| is an IQR-based outlier."""
    col = "mean_seasonal_amplitude"
    if col not in summary_df.columns:
        return summary_df
    amp = summary_df[col].dropna()
    if len(amp) < 4:
        return summary_df
    a = np.abs(amp.to_numpy(dtype=float))
    q1, q3 = np.percentile(a, [25, 75])
    iqr = q3 - q1
    if iqr <= 0:
        return summary_df
    low = q1 - AMPLITUDE_OUTLIER_IQR_MULT * iqr
    high = q3 + AMPLITUDE_OUTLIER_IQR_MULT * iqr
    mask = (summary_df[col].notna()) & (
        (summary_df[col].abs() < low) | (summary_df[col].abs() > high)
    )
    n_removed = mask.sum()
    if n_removed > 0:
        log.debug("remove_amplitude_outliers: removed %d rows (IQR bounds [%.4g, %.4g])", n_removed, low, high)
    return summary_df.loc[~mask].copy()


def plot_ae_distribution(summary_df):
    Theme.apply()
    return _plot_ae_distribution(summary_df)


def plot_amplitude_histogram(summary_df):
    Theme.apply()
    return _plot_amplitude_histogram(summary_df)


def plot_annual_ae(hylak_id, annual_df, trend=None):
    Theme.apply()
    return _plot_annual_ae(hylak_id, annual_df, trend)


def plot_trend_summary(summary_df):
    Theme.apply()
    return _plot_trend_summary(summary_df)


def plot_amplitude_vs_entropy(summary_df):
    Theme.apply()
    return _plot_amplitude_vs_entropy(summary_df)


__all__ = [
    "remove_amplitude_outliers",
    "plot_ae_distribution",
    "plot_amplitude_histogram",
    "plot_annual_ae",
    "plot_trend_summary",
    "plot_amplitude_vs_entropy",
]
