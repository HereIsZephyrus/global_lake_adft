"""Zonal (latitude-profile) visualization for comparison exceedance rates.

Produces a two-panel figure (high / low) showing Quantile vs PWM
exceedance rates as lines with the difference highlighted as a
shaded band between them.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import DEFAULT_VIZ_CONFIG, GlobalGridConfig
from ..grid import zonal_mean
from ..style.base import AxisStyle, apply_axis_style

log = logging.getLogger(__name__)

_VALUE_COLS = [
    "q_high_rate",
    "pwm_high_rate",
    "diff_high_rate",
    "q_low_rate",
    "pwm_low_rate",
    "diff_low_rate",
]

_COL_QUANTILE = "#d8b365"
_COL_PWM = "#5ab4ac"
_FILL_ALPHA = 0.18

_PANEL_DEFS = [
    {
        "q_col": "q_high_rate",
        "pwm_col": "pwm_high_rate",
        "title": "高值超越频率",
        "ylabel": "超越频率",
    },
    {
        "q_col": "q_low_rate",
        "pwm_col": "pwm_low_rate",
        "title": "低值超越频率",
        "ylabel": "超越频率",
    },
]


def _fetch_comparison_exceedance_grid_agg(
    provider, resolution, *, refresh=False, sample_ids=None,
):
    return provider.fetch_grid_agg(
        "comparison.exceedance", resolution, refresh=refresh,
        sample_ids=sample_ids,
    )


def plot_comparison_zonal_profile(
    config: GlobalGridConfig,
    *,
    sample_ids: set[int] | None = None,
    refresh: bool = False,
    min_lakes: int = 1,
    lat_band_size: float = 5.0,
) -> list[Path]:
    """Plot latitude-profile (zonal mean) comparison figures.

    Returns list of output file paths.
    """
    import matplotlib.pyplot as plt

    agg = _fetch_comparison_exceedance_grid_agg(
        config.provider, config.resolution,
        refresh=refresh, sample_ids=sample_ids,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]

    if agg.empty:
        log.warning("No comparison exceedance data available")
        return []

    zonal = zonal_mean(agg, _VALUE_COLS, config.resolution, lat_band_size)
    if zonal.empty:
        log.warning("Zonal aggregation produced no data")
        return []

    out_dir = config.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, 2, figsize=(16, 5.5), sharey=True)

    for ax, panel in zip(axes, _PANEL_DEFS):
        q_vals = zonal[panel["q_col"]].to_numpy()
        pwm_vals = zonal[panel["pwm_col"]].to_numpy()
        lats = zonal["lat_band"].to_numpy()

        ax.fill_between(
            lats, q_vals, pwm_vals,
            alpha=_FILL_ALPHA, color="grey", label="差异",
        )
        ax.plot(
            lats, q_vals,
            color=_COL_QUANTILE, linewidth=2,
            marker="o", markersize=4,
            markerfacecolor="white", markeredgewidth=1.5,
            label="Quantile",
        )
        ax.plot(
            lats, pwm_vals,
            color=_COL_PWM, linewidth=2,
            marker="o", markersize=4,
            markerfacecolor="white", markeredgewidth=1.5,
            label="PWM",
        )

        ax_style = AxisStyle(
            xlabel="纬度 (°)",
            ylabel=panel["ylabel"],
            title=panel["title"],
            grid_alpha=0.3,
        )
        apply_axis_style(ax, ax_style)
        ax.legend(fontsize=10)

    fig.suptitle(
        "Quantile vs PWM 超越频率纬度剖面",
        fontsize=14, fontweight="bold",
    )
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    out_path = out_dir / "comparison_zonal_profile_combined.png"
    fig.savefig(out_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", out_path)

    return [out_path]
