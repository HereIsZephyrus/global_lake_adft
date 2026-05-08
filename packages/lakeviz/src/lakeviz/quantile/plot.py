"""Backward-compatible figure wrappers for the monthly anomaly workflow."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakeviz.domain.quantile import (
    plot_adft_fallback as _plot_adft_fallback,
    plot_anomaly_timeline as _plot_anomaly_timeline,
    plot_monthly_timeline as _plot_monthly_timeline,
    plot_transition_count_summary as _plot_transition_count_summary,
    plot_transition_count_summary_precomputed as _plot_transition_count_summary_precomputed,
    plot_transition_seasonality_summary as _plot_transition_seasonality_summary,
    plot_transition_seasonality_summary_precomputed as _plot_transition_seasonality_summary_precomputed,
)
from lakeviz.layout import save as _save
from lakeviz.style.presets import Theme


def plot_monthly_timeline(labels_df, transitions_df, *, hylak_id=None):
    Theme.apply()
    return _plot_monthly_timeline(labels_df, transitions_df, hylak_id=hylak_id)


def plot_anomaly_timeline(labels_df, *, hylak_id=None):
    Theme.apply()
    return _plot_anomaly_timeline(labels_df, hylak_id=hylak_id)


def plot_transition_count_summary(transitions_df):
    Theme.apply()
    return _plot_transition_count_summary(transitions_df)


def plot_transition_count_summary_precomputed(counts_df):
    Theme.apply()
    return _plot_transition_count_summary_precomputed(counts_df)


def plot_transition_seasonality_summary(transitions_df):
    Theme.apply()
    return _plot_transition_seasonality_summary(transitions_df)


def plot_transition_seasonality_summary_precomputed(seasonality_df):
    Theme.apply()
    return _plot_transition_seasonality_summary_precomputed(seasonality_df)


def plot_adft_fallback(hylak_id, series_df, adft_df):
    Theme.apply()
    return _plot_adft_fallback(hylak_id, series_df, adft_df)


def save_lake_plots(
    labels_df: pd.DataFrame,
    transitions_df: pd.DataFrame,
    output_root: Path,
    *,
    hylak_id: int | None = None,
) -> dict[str, Path]:
    """Save the two required single-lake plots."""
    Theme.apply()
    lake_name = "unknown" if hylak_id is None else str(hylak_id)
    output_dir = output_root / "lakes" / lake_name
    output_dir.mkdir(parents=True, exist_ok=True)

    monthly_path = output_dir / "monthly_timeline.png"
    anomaly_path = output_dir / "anomaly_timeline.png"

    monthly_fig = plot_monthly_timeline(labels_df, transitions_df, hylak_id=hylak_id)
    _save(monthly_fig, monthly_path)

    anomaly_fig = plot_anomaly_timeline(labels_df, hylak_id=hylak_id)
    _save(anomaly_fig, anomaly_path)

    return {
        "monthly_timeline": monthly_path,
        "anomaly_timeline": anomaly_path,
    }


def save_summary_plots(transitions_df: pd.DataFrame, output_root: Path) -> dict[str, Path]:
    """Save the required cross-lake summary plots."""
    Theme.apply()
    output_dir = output_root / "summary"
    output_dir.mkdir(parents=True, exist_ok=True)

    count_path = output_dir / "transition_count_summary.png"
    seasonality_path = output_dir / "transition_seasonality.png"

    count_fig = plot_transition_count_summary(transitions_df)
    _save(count_fig, count_path)

    seasonality_fig = plot_transition_seasonality_summary(transitions_df)
    _save(seasonality_fig, seasonality_path)

    return {
        "transition_count_summary": count_path,
        "transition_seasonality": seasonality_path,
    }

__all__ = [
    "plot_monthly_timeline",
    "plot_anomaly_timeline",
    "plot_transition_count_summary",
    "plot_transition_count_summary_precomputed",
    "plot_transition_seasonality_summary",
    "plot_transition_seasonality_summary_precomputed",
    "save_lake_plots",
    "save_summary_plots",
    "plot_adft_fallback",
]
