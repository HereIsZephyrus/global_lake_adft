"""Backward-compatible figure wrappers for area comparison plots."""

from __future__ import annotations

from lakeviz.style.presets import Theme
from lakeviz.domain.quality import (
    plot_anomaly_upset as _plot_anomaly_upset,
    plot_area_ratio_distribution as _plot_area_ratio_distribution,
    plot_area_scatter as _plot_area_scatter,
    plot_lake_area_grid as _plot_lake_area_grid,
    plot_ratio_histogram as _plot_ratio_histogram,
)


def plot_anomaly_upset(flags_df, *, min_size=0, show_counts=True, title="异常集合交集"):
    Theme.apply()
    return _plot_anomaly_upset(flags_df, min_size=min_size, show_counts=show_counts, title=title)


def plot_area_ratio_distribution(df, **kwargs):
    Theme.apply()
    return _plot_area_ratio_distribution(df, **kwargs)


def plot_area_scatter(df, **kwargs):
    Theme.apply()
    return _plot_area_scatter(df, **kwargs)


def plot_lake_area_grid(lake_data, atlas_areas, ratio_values, *, title="遥感面积差异湖泊抽样"):
    Theme.apply()
    return _plot_lake_area_grid(lake_data, atlas_areas, ratio_values, title=title)


def plot_ratio_histogram(df, **kwargs):
    Theme.apply()
    return _plot_ratio_histogram(df, **kwargs)


__all__ = [
    "plot_anomaly_upset",
    "plot_area_ratio_distribution",
    "plot_area_scatter",
    "plot_lake_area_grid",
    "plot_ratio_histogram",
]
