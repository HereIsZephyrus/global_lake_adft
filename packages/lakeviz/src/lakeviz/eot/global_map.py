"""Global distribution maps for EOT results using SQL-side aggregation + pcolormesh."""

from __future__ import annotations

import logging
from pathlib import Path

from lakesource.config import SourceConfig
from lakesource.eot.reader import (
    fetch_eot_convergence_grid_agg,
    fetch_eot_converged_grid_agg,
)

from ..config import GlobalGridConfig
from ..grid import agg_to_grid_matrix
from ..map_plot import plot_global_grid

log = logging.getLogger(__name__)


def _output_dir(base: Path, tail: str, threshold_quantile: float) -> Path:
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
    agg = fetch_eot_convergence_grid_agg(
        config.source, tail, threshold_quantile, config.resolution,
        refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No EOT convergence data for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "convergence_rate", config.resolution)
    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 收敛率 — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "convergence_rate.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="RdYlGn", log_scale=False,
        vmin=0, vmax=1, cbar_label="收敛率",
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
    agg = fetch_eot_converged_grid_agg(
        config.source, tail, threshold_quantile, config.resolution,
        refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No converged EOT data for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "median_xi", config.resolution)
    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 中位数 ξ — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "median_xi.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="RdBu_r", log_scale=False,
        cbar_label="中位数 ξ",
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
    agg = fetch_eot_converged_grid_agg(
        config.source, tail, threshold_quantile, config.resolution,
        refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No converged EOT data for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "median_sigma", config.resolution)
    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 中位数 σ — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "median_sigma.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="YlOrRd", log_scale=True,
        cbar_label="中位数 σ",
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
    agg = fetch_eot_converged_grid_agg(
        config.source, tail, threshold_quantile, config.resolution,
        refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No converged EOT data for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "mean_extremes_freq", config.resolution)
    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 极端事件频率 — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "extremes_frequency.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="YlOrRd", log_scale=True,
        cbar_label="极端事件频率",
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
    agg = fetch_eot_converged_grid_agg(
        config.source, tail, threshold_quantile, config.resolution,
        refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No converged EOT data for tail=%s q=%.4f", tail, threshold_quantile)
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "median_threshold", config.resolution)
    q_tag = f"q{threshold_quantile:.2f}"
    title = f"EOT 中位数阈值 — {tail} tail, {q_tag}"
    out = _output_dir(config.output_dir, tail, threshold_quantile) / "median_threshold.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="YlOrRd", log_scale=True,
        cbar_label="中位数阈值",
        output_path=out,
    )
    return out
