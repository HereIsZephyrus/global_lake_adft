"""Backward-compatible figure wrappers for area comparison plots."""

from __future__ import annotations

from lakeviz.domain.quality import (
    plot_anomaly_upset,
    plot_area_ratio_distribution,
    plot_area_scatter,
    plot_lake_area_grid,
    plot_ratio_histogram,
)


__all__ = [
    "plot_anomaly_upset",
    "plot_area_ratio_distribution",
    "plot_area_scatter",
    "plot_lake_area_grid",
    "plot_ratio_histogram",
]
