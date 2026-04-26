"""Cartopy-based global distribution maps using pcolormesh.

``draw_global_grid`` operates on a single Axes (geographic projection).
``plot_global_grid`` is the backward-compatible convenience wrapper.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from lakeviz.config import DEFAULT_VIZ_CONFIG
from lakeviz.style.base import AxKind, stamp_ax

log = logging.getLogger(__name__)


def draw_global_grid(
    ax: plt.Axes,
    lons: np.ndarray,
    lats: np.ndarray,
    values: np.ndarray,
    *,
    title: str = "",
    cmap: str = "YlOrRd",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    cbar_label: str = "",
) -> None:
    """Draw a global grid map on *ax* using pcolormesh.

    The Axes must already have a Cartopy projection (e.g. Robinson).
    This function stamps ``ax._ax_kind = AxKind.GEOGRAPHIC``.
    """
    stamp_ax(ax, AxKind.GEOGRAPHIC)

    ax.add_feature(cfeature.OCEAN, facecolor="#e8f4f8", edgecolor="none")
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", edgecolor="none")
    ax.add_feature(cfeature.LAKES, facecolor="#d4e6f1", edgecolor="#666666", linewidth=0.2)
    ax.set_global()

    valid = values[~np.isnan(values)]
    if len(valid) == 0:
        ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")
        if title:
            ax.set_title(title, fontsize=14)
        return

    _vmin = vmin if vmin is not None else valid[valid > 0].min() if (valid > 0).any() else 0.1
    _vmax = vmax if vmax is not None else valid.max()

    if log_scale and _vmin > 0:
        norm = mcolors.LogNorm(vmin=_vmin, vmax=_vmax)
    else:
        norm = mcolors.Normalize(vmin=_vmin, vmax=_vmax)

    mesh = ax.pcolormesh(
        lons, lats, values,
        transform=ccrs.PlateCarree(),
        norm=norm,
        cmap=cmap,
        shading="auto",
    )

    ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")

    fig = ax.get_figure()
    cbar = fig.colorbar(
        mesh, ax=ax, orientation="horizontal", pad=0.05, shrink=0.6, aspect=30,
        drawedges=True, extendrect=True,
    )
    cbar.set_label(cbar_label, fontsize=10)

    if title:
        ax.set_title(title, fontsize=14)


def plot_global_grid(
    lons: np.ndarray,
    lats: np.ndarray,
    values: np.ndarray,
    title: str = "",
    cmap: str = "YlOrRd",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    cbar_label: str = "",
    output_path: Path | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(
        figsize=(16, 8),
        subplot_kw={"projection": ccrs.Robinson()},
    )
    draw_global_grid(
        ax, lons, lats, values,
        title=title, cmap=cmap, log_scale=log_scale,
        vmin=vmin, vmax=vmax, cbar_label=cbar_label,
    )
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=DEFAULT_VIZ_CONFIG.default_dpi, bbox_inches="tight")
        log.info("Saved figure to %s", output_path)
    return fig