"""Global distribution maps for monthly transition (quantile-based) results.

Maps:
  - extremes density: number of extreme events per lake per grid cell
  - extremes by type: dry/wet event density
  - transition density: number of abrupt transitions per lake per grid cell
  - transition by type: dry-to-wet / wet-to-dry density
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.quantile.reader import (
    fetch_extremes_with_coords,
    fetch_transitions_with_coords,
)

from ..config import GlobalGridConfig
from ..grid import build_grid_counts, build_grid_stats
from ..map_plot import plot_global_grid

log = logging.getLogger(__name__)


def _output_dir(base: Path, sub: str) -> Path:
    d = base / "quantile" / sub
    d.mkdir(parents=True, exist_ok=True)
    return d


def plot_extremes_density_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    """Global map of extreme events per lake per grid cell.

    Args:
        config: Grid visualization config.
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum lake count per cell.

    Returns:
        Path to saved figure.
    """
    df = fetch_extremes_with_coords(config.source, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No monthly transition extremes data")
        return Path()

    grid = build_grid_counts(df, resolution=config.resolution)
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    title = "分位数识别极端事件密度 (每湖事件数)"
    out = _output_dir(config.output_dir, "extremes") / "density.png"

    plot_global_grid(
        grid,
        value_col="mean_per_lake",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out


def plot_extremes_by_type_map(
    config: GlobalGridConfig,
    event_type: str,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    """Global map of extreme events per lake for a specific event type.

    Args:
        config: Grid visualization config.
        event_type: "dry" or "wet".
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum lake count per cell.

    Returns:
        Path to saved figure.
    """
    df = fetch_extremes_with_coords(config.source, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No monthly transition extremes data")
        return Path()

    df = df[df["event_type"] == event_type].reset_index(drop=True)
    if df.empty:
        log.warning("No extremes with event_type=%s", event_type)
        return Path()

    grid = build_grid_counts(df, resolution=config.resolution)
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    label = "干旱" if event_type == "dry" else "湿润"
    title = f"分位数识别{label}极端事件密度 (每湖事件数)"
    out = _output_dir(config.output_dir, "extremes") / f"density_{event_type}.png"

    plot_global_grid(
        grid,
        value_col="mean_per_lake",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out


def plot_transition_density_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    """Global map of abrupt transitions per lake per grid cell.

    Args:
        config: Grid visualization config.
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum lake count per cell.

    Returns:
        Path to saved figure.
    """
    df = fetch_transitions_with_coords(config.source, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No monthly transition data")
        return Path()

    grid = build_grid_counts(df, resolution=config.resolution)
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    title = "分位数识别旱涝突变密度 (每湖事件数)"
    out = _output_dir(config.output_dir, "transitions") / "density.png"

    plot_global_grid(
        grid,
        value_col="mean_per_lake",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out


def plot_transition_by_type_map(
    config: GlobalGridConfig,
    transition_type: str,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    """Global map of abrupt transitions per lake for a specific transition type.

    Args:
        config: Grid visualization config.
        transition_type: e.g. "dry_to_wet" or "wet_to_dry".
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum lake count per cell.

    Returns:
        Path to saved figure.
    """
    df = fetch_transitions_with_coords(config.source, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No monthly transition data")
        return Path()

    df = df[df["transition_type"] == transition_type].reset_index(drop=True)
    if df.empty:
        log.warning("No transitions with transition_type=%s", transition_type)
        return Path()

    grid = build_grid_counts(df, resolution=config.resolution)
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    labels = {"dry_to_wet": "旱转涝", "wet_to_dry": "涝转旱"}
    label = labels.get(transition_type, transition_type)
    title = f"分位数识别{label}突变密度 (每湖事件数)"
    out = _output_dir(config.output_dir, "transitions") / f"density_{transition_type}.png"

    plot_global_grid(
        grid,
        value_col="mean_per_lake",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out
