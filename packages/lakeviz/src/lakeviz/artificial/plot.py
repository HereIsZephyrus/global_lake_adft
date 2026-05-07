"""Backward-compatible figure wrappers for artificial lake impact plots."""

from __future__ import annotations

from lakeviz.style.presets import Theme
from lakeviz.domain.artificial import (
    plot_anomaly_ratio_comparison as _plot_anomaly_ratio_comparison,
    plot_delta_cv_distribution as _plot_delta_cv_distribution,
    plot_typical_pair_timeline as _plot_typical_pair_timeline,
    plot_volatility_comparison as _plot_volatility_comparison,
)


def plot_anomaly_ratio_comparison(impact_df):
    Theme.apply()
    return _plot_anomaly_ratio_comparison(impact_df)


def plot_delta_cv_distribution(impact_df):
    Theme.apply()
    return _plot_delta_cv_distribution(impact_df)


def plot_typical_pair_timeline(df_a, df_n, pair_info):
    Theme.apply()
    return _plot_typical_pair_timeline(df_a, df_n, pair_info)


def plot_volatility_comparison(impact_df):
    Theme.apply()
    return _plot_volatility_comparison(impact_df)

__all__ = [
    "plot_anomaly_ratio_comparison",
    "plot_delta_cv_distribution",
    "plot_typical_pair_timeline",
    "plot_volatility_comparison",
]
