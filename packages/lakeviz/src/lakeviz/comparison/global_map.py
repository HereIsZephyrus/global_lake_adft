"""Comparison global distribution maps: Quantile vs PWM exceedance rates."""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from ..config import DEFAULT_VIZ_CONFIG, GlobalGridConfig
from ..grid import agg_to_grid_matrix
from ..layout import create_figure, save
from ..map_plot import draw_global_grid

log = logging.getLogger(__name__)


def _fetch_comparison_exceedance_grid_agg(
    provider, resolution, *, refresh=False, sample_ids=None,
):
    return provider.fetch_grid_agg(
        "comparison.exceedance", resolution, refresh=refresh,
        sample_ids=sample_ids,
    )


def _draw_and_save(
    agg, value_col, resolution, output_dir, sub_dir, filename,
    title, cmap, log_scale, vmin, vmax, cbar_label,
):
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    if agg.empty or value_col not in agg.columns:
        log.warning("No data for %s", title)
        return Path()

    lons, lats, values = agg_to_grid_matrix(agg, value_col, resolution)

    out_path = output_dir / sub_dir / filename
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(14, 7), subplot_kw={"projection": ccrs.Robinson()})
    draw_global_grid(
        ax, lons, lats, values,
        title=title, cmap=cmap, log_scale=log_scale,
        vmin=vmin, vmax=vmax, cbar_label=cbar_label,
    )
    fig.savefig(out_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s → %s", title, out_path)
    return out_path


_PLOT_SPECS = [
    (
        "q_high_rate", "Quantile 高值超越频率",
        "超越率", "sequential_warm", True, None, None,
        "q_high_rate.png",
    ),
    (
        "pwm_high_rate", "PWM 高值超越频率",
        "超越率", "sequential_warm", True, None, None,
        "pwm_high_rate.png",
    ),
    (
        "diff_high_rate", "高值超越频率差异 (PWM - Quantile)",
        "差异", "BlueWhiteOrangeRed", False, None, None,
        "diff_high_rate.png",
    ),
    (
        "q_low_rate", "Quantile 低值超越频率",
        "超越率", "sequential_warm", True, None, None,
        "q_low_rate.png",
    ),
    (
        "pwm_low_rate", "PWM 低值超越频率",
        "超越率", "sequential_warm", True, None, None,
        "pwm_low_rate.png",
    ),
    (
        "diff_low_rate", "低值超越频率差异 (PWM - Quantile)",
        "差异", "BlueWhiteOrangeRed", False, None, None,
        "diff_low_rate.png",
    ),
]


def plot_comparison_exceedance_maps(
    config: GlobalGridConfig,
    *,
    sample_ids: set[int] | None = None,
    refresh: bool = False,
    min_lakes: int = 1,
) -> list[Path]:
    agg = _fetch_comparison_exceedance_grid_agg(
        config.provider, config.resolution,
        refresh=refresh, sample_ids=sample_ids,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]

    if agg.empty:
        log.warning("No comparison exceedance data available")
        return []

    paths: list[Path] = []
    for value_col, title, cbar_label, cmap, log_scale, vmin, vmax, filename in _PLOT_SPECS:
        out = _draw_and_save(
            agg, value_col, config.resolution, config.output_dir,
            sub_dir="comparison", filename=filename,
            title=title, cmap=cmap, log_scale=log_scale,
            vmin=vmin, vmax=vmax, cbar_label=cbar_label,
        )
        if out and out.exists():
            paths.append(out)
        else:
            log.warning("Skipped: %s (no data)", value_col)

    return paths


def plot_comparison_exceedance_panel(
    config: GlobalGridConfig,
    *,
    sample_ids: set[int] | None = None,
    refresh: bool = False,
    min_lakes: int = 1,
) -> list[Path]:
    """Generate three 1×2 panels of comparison exceedance maps.

    Each row produces a separate figure with its own colorbar:

        Row 0: q_high_rate  | q_low_rate   [warm cbar]
        Row 1: pwm_high_rate | pwm_low_rate [warm cbar]
        Row 2: diff_high_rate | diff_low_rate [div cbar]

    Returns:
        List of output paths for the three panel figures.
    """
    import cartopy.crs as ccrs

    agg = _fetch_comparison_exceedance_grid_agg(
        config.provider, config.resolution,
        refresh=refresh, sample_ids=sample_ids,
    )
    if min_lakes > 1:
        agg = agg[agg["lake_count"] >= min_lakes]

    if agg.empty:
        log.warning("No comparison exceedance data available")
        return []

    projection = ccrs.Robinson()
    row_groups: dict[int, list[tuple]] = {0: [], 1: [], 2: []}

    for idx, spec in enumerate(_PLOT_SPECS):
        row = idx // 2
        row_groups[row].append(spec)

    row_labels = {
        0: ("Quantile vs PWM 高值", "quantile_pwm_high.png"),
        1: ("Quantile vs PWM 低值", "quantile_pwm_low.png"),
        2: ("超越频率差异", "diff_rate.png"),
    }

    paths: list[Path] = []

    for row, specs in row_groups.items():
        if not specs:
            continue

        fig, axes = create_figure(
            [
                {"name": "left", "row": 0, "col": 0},
                {"name": "right", "row": 0, "col": 1},
            ],
            figsize=(12, 5),
            width_ratios=[1, 1],
            projection=projection,
        )

        metas = []
        cmap_name = specs[0][3]
        ax_names = ["left", "right"]

        for col, (
            value_col, title, cbar_label, cmap, log_scale, vmin, vmax, _fn,
        ) in enumerate(specs):
            if value_col not in agg.columns:
                log.warning("No data for %s", title)
                continue

            lons, lats, values = agg_to_grid_matrix(
                agg, value_col, config.resolution,
            )

            ax = axes[ax_names[col]]
            meta = draw_global_grid(
                ax, lons, lats, values,
                title=title, cmap=cmap, log_scale=log_scale,
                vmin=vmin, vmax=vmax, cbar_label=cbar_label,
                add_cbar=False,
            )

            if meta is not None:
                metas.append(meta)

        if metas:
            label = "超越率" if cmap_name == "sequential_warm" else "差异"
            _add_row_cbar(fig, axes["right"], metas, cmap_name=cmap_name, label=label)

        suptitle, fn = row_labels[row]
        fig.suptitle(suptitle, fontsize=14, y=0.98)

        out_path = config.output_dir / "comparison" / fn
        paths.append(save(fig, out_path))

    return paths


def _add_row_cbar(fig, right_ax, metas, *, cmap_name, label=""):
    """Add a shared vertical colorbar placed to the right of *right_ax*.

    Position is derived from ``right_ax.get_position()`` — no GridSpec dependency.
    """
    import matplotlib.colors as mcolors
    import matplotlib.ticker as mticker

    from ..style.presets import resolve_cmap

    resolved_cmap = resolve_cmap(cmap_name)

    vmin_shared = min(m["vmin"] for m in metas)
    vmax_shared = max(m["vmax"] for m in metas)
    log_scale = metas[0]["log_scale"]

    n_levels = len(metas[0]["bounds"]) - 1
    if log_scale and vmin_shared > 0:
        bounds = np.logspace(
            np.log10(vmin_shared), np.log10(vmax_shared), n_levels + 1,
        )
    else:
        bounds = np.linspace(vmin_shared, vmax_shared, n_levels + 1)
    norm = mcolors.BoundaryNorm(bounds, ncolors=n_levels)

    for meta in metas:
        meta["mesh"].set_norm(norm)
        meta["mesh"].set_cmap(resolved_cmap)

    bbox = right_ax.get_position()
    cbar_width = 0.015
    gap = 0.01
    cbar_ax = fig.add_axes([bbox.x1 + gap, bbox.y0, cbar_width, bbox.y1 - bbox.y0])
    sm = plt.cm.ScalarMappable(cmap=resolved_cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, cax=cbar_ax, extendrect=True, extendfrac="auto")
    cbar.set_ticks(bounds)
    cbar.ax.tick_params(labelsize=8)
    if log_scale and vmin_shared > 0:
        cbar.ax.yaxis.set_major_formatter(
            mticker.LogFormatterSciNotation(labelOnlyBase=False),
        )
    if label:
        cbar.ax.set_ylabel(label, fontsize=9)
