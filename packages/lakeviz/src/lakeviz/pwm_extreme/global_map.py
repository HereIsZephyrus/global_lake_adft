"""Global distribution maps for PWM extreme quantile results."""

from __future__ import annotations

import logging
from pathlib import Path

from lakesource.config import SourceConfig
from lakesource.pwm_extreme.reader import (
    fetch_pwm_convergence_grid_agg,
    fetch_pwm_converged_grid_agg,
)

from ..config import GlobalGridConfig
from ..grid import agg_to_grid_matrix
from ..map_plot import plot_global_grid

log = logging.getLogger(__name__)


def _output_dir(base: Path, sub: str) -> Path:
    d = base / "pwm_extreme" / sub
    d.mkdir(parents=True, exist_ok=True)
    return d


def plot_pwm_convergence_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    agg = fetch_pwm_convergence_grid_agg(
        config.source, config.resolution, refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No PWM extreme convergence data")
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "convergence_rate", config.resolution)
    title = "PWM 极端分位数收敛率"
    out = _output_dir(config.output_dir, "") / "convergence_rate.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="RdYlGn", log_scale=False,
        vmin=0, vmax=1, cbar_label="收敛率",
        output_path=out,
    )
    return out


def plot_pwm_threshold_high_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    agg = fetch_pwm_converged_grid_agg(
        config.source, config.resolution, refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No converged PWM extreme data")
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "median_threshold_high", config.resolution)
    title = "PWM 极端分位数中位数高阈值"
    out = _output_dir(config.output_dir, "") / "median_threshold_high.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="YlOrRd", log_scale=True,
        cbar_label="中位数高阈值",
        output_path=out,
    )
    return out


def plot_pwm_threshold_low_map(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    agg = fetch_pwm_converged_grid_agg(
        config.source, config.resolution, refresh=refresh, data_dir=data_dir,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]
    if agg.empty:
        log.warning("No converged PWM extreme data")
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, "median_threshold_low", config.resolution)
    title = "PWM 极端分位数中位数低阈值"
    out = _output_dir(config.output_dir, "") / "median_threshold_low.png"

    plot_global_grid(
        lons, lats, values,
        title=title, cmap="YlOrRd", log_scale=True,
        cbar_label="中位数低阈值",
        output_path=out,
    )
    return out
