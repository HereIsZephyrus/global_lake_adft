"""Global distribution maps for EOT (Empirical Orthogonal Teleconnection) results.

Each function follows the same pipeline:
  1. Load EOT results with lake coordinates (via lakesource, with parquet cache).
  2. Filter to converged fits only (where applicable).
  3. Bin into 0.5-degree grid with custom aggregation.
  4. Render as Cartopy Robinson projection global map.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.config import SourceConfig
from lakesource.eot.reader import fetch_eot_results_with_coords

from ..config import GlobalGridConfig
from ..grid import build_grid_stats
from ..map_plot import plot_global_grid

log = logging.getLogger(__name__)


def _load_converged(
    config: SourceConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    df = fetch_eot_results_with_coords(
        config, tail, threshold_quantile, refresh=refresh, data_dir=data_dir,
    )
    if df.empty:
        return df
    return df[df["converged"] == True].reset_index(drop=True)


def _output_dir(
    base: Path, tail: str, threshold_quantile: float,
) -> Path:
    q_tag = f"q{threshold_quantile:.4f}"
    d = base / "eot" / tail / q_tag
    d.mkdir(parents=True, exist_ok=True)
    return d


def plot_eot_convergence_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    """Global map of EOT convergence rate per grid cell.

    Convergence rate = fraction of lakes in the cell whose EOT fit converged.

    Args:
        config: Grid visualization config.
        tail: "high" or "low".
        threshold_quantile: Quantile level (e.g. 0.95).
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum lake count per cell to include.

    Returns:
        Path to saved figure.
    """
    df = fetch_eot_results_with_coords(
        config.source, tail, threshold_quantile, refresh=refresh, data_dir=data_dir,
    )
    if df.empty:
        log.warning("No EOT results for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    grid = build_grid_stats(
        df,
        agg_specs={"convergence_rate": ("converged", "mean")},
        resolution=config.resolution,
    )
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 收敛率 — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "convergence_rate.png"

    plot_global_grid(
        grid,
        value_col="convergence_rate",
        title=title,
        cmap="RdYlGn",
        log_scale=False,
        vmin=0,
        vmax=1,
        output_path=out,
    )
    return out


def plot_eot_xi_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    """Global map of median shape parameter ξ per grid cell (converged fits only).

    Args:
        config: Grid visualization config.
        tail: "high" or "low".
        threshold_quantile: Quantile level.
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum converged lakes per cell.

    Returns:
        Path to saved figure.
    """
    df = _load_converged(config.source, tail, threshold_quantile, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No converged EOT results for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    grid = build_grid_stats(
        df,
        agg_specs={"median_xi": ("xi", "median")},
        resolution=config.resolution,
    )
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 中位数 ξ — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "median_xi.png"

    plot_global_grid(
        grid,
        value_col="median_xi",
        title=title,
        cmap="RdBu_r",
        log_scale=False,
        output_path=out,
    )
    return out


def plot_eot_sigma_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    """Global map of median scale parameter σ per grid cell (converged fits only).

    Args:
        config: Grid visualization config.
        tail: "high" or "low".
        threshold_quantile: Quantile level.
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum converged lakes per cell.

    Returns:
        Path to saved figure.
    """
    df = _load_converged(config.source, tail, threshold_quantile, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No converged EOT results for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    grid = build_grid_stats(
        df,
        agg_specs={"median_sigma": ("sigma", "median")},
        resolution=config.resolution,
    )
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 中位数 σ — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "median_sigma.png"

    plot_global_grid(
        grid,
        value_col="median_sigma",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out


def plot_eot_extremes_frequency_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    """Global map of mean extremes frequency per grid cell (converged fits only).

    Extremes frequency = n_extremes / n_observations (average per lake in cell).

    Args:
        config: Grid visualization config.
        tail: "high" or "low".
        threshold_quantile: Quantile level.
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum converged lakes per cell.

    Returns:
        Path to saved figure.
    """
    df = _load_converged(config.source, tail, threshold_quantile, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No converged EOT results for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    df = df.copy()
    df["extremes_freq"] = df["n_extremes"].astype(float) / df["n_observations"].astype(float)

    grid = build_grid_stats(
        df,
        agg_specs={"mean_extremes_freq": ("extremes_freq", "mean")},
        resolution=config.resolution,
    )
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 极端事件频率 — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "extremes_frequency.png"

    plot_global_grid(
        grid,
        value_col="mean_extremes_freq",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out


def plot_eot_threshold_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    """Global map of median threshold value per grid cell (converged fits only).

    Args:
        config: Grid visualization config.
        tail: "high" or "low".
        threshold_quantile: Quantile level.
        refresh: Re-fetch from database.
        data_dir: Override cache directory.
        min_lakes: Minimum converged lakes per cell.

    Returns:
        Path to saved figure.
    """
    df = _load_converged(config.source, tail, threshold_quantile, refresh=refresh, data_dir=data_dir)
    if df.empty:
        log.warning("No converged EOT results for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    grid = build_grid_stats(
        df,
        agg_specs={"median_threshold": ("threshold", "median")},
        resolution=config.resolution,
    )
    if min_lakes > 1:
        grid = grid[grid["lake_count"] >= min_lakes]

    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 中位数阈值 — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "median_threshold.png"

    plot_global_grid(
        grid,
        value_col="median_threshold",
        title=title,
        cmap="YlOrRd",
        log_scale=True,
        output_path=out,
    )
    return out
