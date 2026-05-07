"""Backward-compatible figure wrappers for lake-pair similarity plots."""

from __future__ import annotations

from lakeviz.style.presets import Theme
from lakeviz.domain.similarity import (
    plot_acf_cosine_distribution as _plot_acf_cosine_distribution,
    plot_pearson_distribution as _plot_pearson_distribution,
    plot_pearson_vs_acf as _plot_pearson_vs_acf,
)


def plot_pearson_distribution(summary_df):
    Theme.apply()
    return _plot_pearson_distribution(summary_df)


def plot_acf_cosine_distribution(summary_df):
    Theme.apply()
    return _plot_acf_cosine_distribution(summary_df)


def plot_pearson_vs_acf(summary_df):
    Theme.apply()
    return _plot_pearson_vs_acf(summary_df)

__all__ = [
    "plot_pearson_distribution",
    "plot_acf_cosine_distribution",
    "plot_pearson_vs_acf",
]
