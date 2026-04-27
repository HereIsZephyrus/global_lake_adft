"""Global distribution maps for PWM extreme quantile results."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from ..config import DEFAULT_VIZ_CONFIG, GlobalGridConfig
from ..grid import agg_to_grid_matrix
from ..grid_map_factory import make_grid_map
from ..map_plot import draw_global_grid

log = logging.getLogger(__name__)


def _fetch_pwm_convergence_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_pwm_convergence_grid_agg(resolution, refresh=refresh)


def _fetch_pwm_converged_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_pwm_converged_grid_agg(resolution, refresh=refresh)


def _fetch_pwm_monthly_threshold_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_pwm_monthly_threshold_grid_agg(resolution, refresh=refresh)


def _fetch_pwm_exceedance_grid_agg(provider, resolution, *, p_high=0.05, p_low=0.05, refresh=False):
    return provider.fetch_pwm_exceedance_grid_agg(resolution, p_high=p_high, p_low=p_low, refresh=refresh)


def _fetch_pwm_monthly_exceedance_grid_agg(provider, resolution, *, p_high=0.05, p_low=0.05, refresh=False):
    return provider.fetch_pwm_monthly_exceedance_grid_agg(resolution, p_high=p_high, p_low=p_low, refresh=refresh)


plot_pwm_convergence_map = make_grid_map(
    _fetch_pwm_convergence_grid_agg,
    "convergence_rate",
    title="PWM 极端分位数收敛率",
    cmap="RdYlGn",
    log_scale=False,
    vmin=0,
    vmax=1,
    cbar_label="收敛率",
    sub_dir="pwm_extreme",
    filename="convergence_rate.png",
)

plot_pwm_threshold_high_map = make_grid_map(
    _fetch_pwm_converged_grid_agg,
    "median_threshold_high",
    title="PWM 极端分位数中位数高阈值",
    cbar_label="中位数高阈值",
    sub_dir="pwm_extreme",
    filename="median_threshold_high.png",
)

plot_pwm_threshold_low_map = make_grid_map(
    _fetch_pwm_converged_grid_agg,
    "median_threshold_low",
    title="PWM 极端分位数中位数低阈值",
    cbar_label="中位数低阈值",
    sub_dir="pwm_extreme",
    filename="median_threshold_low.png",
)


_MONTH_NAMES = [
    "", "1月", "2月", "3月", "4月", "5月", "6月",
    "7月", "8月", "9月", "10月", "11月", "12月",
]


def plot_pwm_monthly_threshold_maps(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
) -> list[Path]:
    """Generate 24 monthly threshold maps (12 months x high/low).

    All months share a unified color scale per threshold type so that
    cross-month comparison is meaningful.

    Returns:
        List of output paths for all generated maps.
    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    agg = _fetch_pwm_monthly_threshold_grid_agg(
        config.provider, config.resolution, refresh=refresh
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]

    if agg.empty:
        log.warning("No monthly threshold data available")
        return []

    paths: list[Path] = []

    for value_col, threshold_label, sub_prefix in [
        ("median_threshold_high", "高阈值", "threshold_high"),
        ("median_threshold_low", "低阈值", "threshold_low"),
    ]:
        sub_agg = agg[["month", "cell_lat", "cell_lon", "lake_count", value_col]].copy()
        valid = sub_agg[value_col].dropna()
        if valid.empty:
            log.warning("No valid data for %s", value_col)
            continue

        vmin = float(valid[valid > 0].min()) if (valid > 0).any() else 0.1
        vmax = float(valid.max())

        for month in range(1, 13):
            month_agg = sub_agg[sub_agg["month"] == month].reset_index(drop=True)
            if month_agg.empty:
                continue

            lons, lats, values = agg_to_grid_matrix(
                month_agg, value_col, config.resolution
            )

            out_dir = config.output_dir / "pwm_extreme" / "monthly" / sub_prefix
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"month_{month:02d}.png"

            fig, ax = plt.subplots(
                figsize=(16, 8),
                subplot_kw={"projection": ccrs.Robinson()},
            )
            draw_global_grid(
                ax, lons, lats, values,
                title=f"PWM 极端分位数{threshold_label} ({_MONTH_NAMES[month]})",
                cmap="YlOrRd",
                log_scale=True,
                vmin=vmin,
                vmax=vmax,
                cbar_label=f"中位数{threshold_label}",
            )
            fig.savefig(out_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
            plt.close(fig)
            log.info("Saved: PWM %s month %d → %s", threshold_label, month, out_path)
            paths.append(out_path)

    return paths


DEFAULT_P_VALUES = [0.01, 0.025, 0.05, 0.10]


def plot_pwm_exceedance_maps(
    config: GlobalGridConfig,
    *,
    p_values: list[float] | None = None,
    refresh: bool = False,
    min_lakes: int = 1,
) -> list[Path]:
    """Generate exceedance maps for multiple p values.

    For each p in p_values, produces 4 aggregate maps (mean/median x high/low)
    using the cross-entropy quantile function to recompute thresholds.

    Returns:
        List of output paths for all generated maps.
    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    if p_values is None:
        p_values = DEFAULT_P_VALUES

    paths: list[Path] = []

    for p in p_values:
        p_tag = f"p{p:.4f}"
        agg = _fetch_pwm_exceedance_grid_agg(
            config.provider, config.resolution, p_high=p, p_low=p, refresh=refresh
        )
        if min_lakes > 1:
            agg = agg[agg["lake_count"] >= min_lakes]

        if agg.empty:
            log.warning("No exceedance data for p=%s", p)
            continue

        for value_col, label, filename in [
            ("mean_high_exceedance", "平均高阈值超越月数", "mean_high_exceedance.png"),
            ("mean_low_exceedance", "平均低阈值超越月数", "mean_low_exceedance.png"),
            ("median_high_exceedance", "中位数高阈值超越月数", "median_high_exceedance.png"),
            ("median_low_exceedance", "中位数低阈值超越月数", "median_low_exceedance.png"),
        ]:
            lons, lats, values = agg_to_grid_matrix(agg, value_col, config.resolution)

            out_dir = config.output_dir / "pwm_extreme" / "exceedance" / p_tag
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / filename

            fig, ax = plt.subplots(
                figsize=(16, 8),
                subplot_kw={"projection": ccrs.Robinson()},
            )
            draw_global_grid(
                ax, lons, lats, values,
                title=f"PWM {label} (p={p})",
                cmap="YlOrRd",
                log_scale=True,
                cbar_label=label,
            )
            fig.savefig(out_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
            plt.close(fig)
            log.info("Saved: PWM p=%s %s → %s", p, label, out_path)
            paths.append(out_path)

    return paths


def plot_pwm_monthly_exceedance_maps(
    config: GlobalGridConfig,
    *,
    p_values: list[float] | None = None,
    refresh: bool = False,
    min_lakes: int = 1,
) -> list[Path]:
    """Generate monthly exceedance rate maps for multiple p values.

    For each p in p_values, produces 24 maps (12 months x high/low rate).

    Returns:
        List of output paths for all generated maps.
    """
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    if p_values is None:
        p_values = DEFAULT_P_VALUES

    paths: list[Path] = []

    for p in p_values:
        p_tag = f"p{p:.4f}"
        agg = _fetch_pwm_monthly_exceedance_grid_agg(
            config.provider, config.resolution, p_high=p, p_low=p, refresh=refresh
        )
        if min_lakes > 1:
            agg = agg[agg["lake_count"] >= min_lakes]

        if agg.empty:
            log.warning("No monthly exceedance data for p=%s", p)
            continue

        for value_col, rate_label, sub_prefix in [
            ("high_exceedance_rate", "高阈值超越率", "high_rate"),
            ("low_exceedance_rate", "低阈值超越率", "low_rate"),
        ]:
            sub_agg = agg[["month", "cell_lat", "cell_lon", "lake_count", value_col]].copy()
            valid = sub_agg[value_col].dropna()
            if valid.empty:
                log.warning("No valid data for %s at p=%s", value_col, p)
                continue

            for month in range(1, 13):
                month_agg = sub_agg[sub_agg["month"] == month].reset_index(drop=True)
                if month_agg.empty:
                    continue

                lons, lats, values = agg_to_grid_matrix(
                    month_agg, value_col, config.resolution
                )

                out_dir = config.output_dir / "pwm_extreme" / "monthly" / p_tag / sub_prefix
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"month_{month:02d}.png"

                fig, ax = plt.subplots(
                    figsize=(16, 8),
                    subplot_kw={"projection": ccrs.Robinson()},
                )
                draw_global_grid(
                    ax, lons, lats, values,
                    title=f"PWM {rate_label} (p={p}, {_MONTH_NAMES[month]})",
                    cmap="YlOrRd",
                    log_scale=False,
                    vmin=0,
                    vmax=1,
                    cbar_label=rate_label,
                )
                fig.savefig(out_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
                plt.close(fig)
                log.info("Saved: PWM p=%s %s month %d → %s", p, rate_label, month, out_path)
                paths.append(out_path)

    return paths