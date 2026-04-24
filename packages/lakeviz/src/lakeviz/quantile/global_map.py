"""Global distribution maps for quantile-based identification results.

Uses SQL-side aggregation + pcolormesh for memory-efficient rendering.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from lakesource.config import SourceConfig
from lakesource.quantile.reader import (
    fetch_extremes_grid_agg,
    fetch_extremes_by_type_grid_agg,
    fetch_transitions_grid_agg,
    fetch_transitions_by_type_grid_agg,
)

from ..config import GlobalGridConfig
from ..grid import agg_to_grid_matrix
from ..map_plot import plot_global_grid

log = logging.getLogger(__name__)


def _output_dir(base: Path, sub: str) -> Path:
    d = base / "quantile" / sub
    d.mkdir(parents=True, exist_ok=True)
    return d


def _plot_density(
    agg_df,
    value_col: str,
    title: str,
    cbar_label: str,
    output_path: Path,
    config: GlobalGridConfig,
) -> Path:
    if agg_df.empty:
        log.warning("No data for %s", title)
        return Path()
    agg_df = agg_df.copy()
    agg_df["mean_per_lake"] = agg_df["event_count"].astype(float) / agg_df["lake_count"].astype(float)
    lons, lats, values = agg_to_grid_matrix(agg_df, value_col, config.resolution)
    plot_global_grid(
        lons, lats, values,
        title=title, cmap="YlOrRd", log_scale=True,
        cbar_label=cbar_label, output_path=output_path,
    )
    return output_path


def plot_extremes_density_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    agg = fetch_extremes_grid_agg(config.source, config.resolution, refresh=refresh, data_dir=data_dir)
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    return _plot_density(
        agg, "mean_per_lake",
        "分位数识别极端事件密度 (每湖事件数)", "每湖事件数",
        _output_dir(config.output_dir, "extremes") / "density.png",
        config,
    )


def plot_extremes_by_type_map(
    config: GlobalGridConfig,
    event_type: str,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    agg = fetch_extremes_by_type_grid_agg(config.source, config.resolution, refresh=refresh, data_dir=data_dir)
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    agg = agg[agg["event_type"] == event_type].reset_index(drop=True)
    labels = {"high": "高值", "low": "低值", "dry": "干旱", "wet": "湿润"}
    label = labels.get(event_type, event_type)
    return _plot_density(
        agg, "mean_per_lake",
        f"分位数识别{label}极端事件密度 (每湖事件数)", "每湖事件数",
        _output_dir(config.output_dir, "extremes") / f"density_{event_type}.png",
        config,
    )


def plot_transition_density_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    agg = fetch_transitions_grid_agg(config.source, config.resolution, refresh=refresh, data_dir=data_dir)
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    return _plot_density(
        agg, "mean_per_lake",
        "分位数识别旱涝突变密度 (每湖事件数)", "每湖事件数",
        _output_dir(config.output_dir, "transitions") / "density.png",
        config,
    )


def plot_transition_by_type_map(
    config: GlobalGridConfig,
    transition_type: str,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    agg = fetch_transitions_by_type_grid_agg(config.source, config.resolution, refresh=refresh, data_dir=data_dir)
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    agg = agg[agg["transition_type"] == transition_type].reset_index(drop=True)
    labels = {"low_to_high": "低转高", "high_to_low": "高转低", "dry_to_wet": "旱转涝", "wet_to_dry": "涝转旱"}
    label = labels.get(transition_type, transition_type)
    return _plot_density(
        agg, "mean_per_lake",
        f"分位数识别{label}突变密度 (每湖事件数)", "每湖事件数",
        _output_dir(config.output_dir, "transitions") / f"density_{transition_type}.png",
        config,
    )
