"""Cartopy-based global distribution maps using pcolormesh."""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature

log = logging.getLogger(__name__)


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
    """Plot a global grid map using pcolormesh with Robinson projection.

    Args:
        lons: 1D array of cell-center longitudes (n_lon,).
        lats: 1D array of cell-center latitudes (n_lat,).
        values: 2D array of shape (n_lat, n_lon), NaN where no data.
        title: Figure title.
        cmap: Matplotlib colormap name.
        log_scale: Whether to use logarithmic color scale.
        vmin: Minimum value for color scale (auto if None).
        vmax: Maximum value for color scale (auto if None).
        cbar_label: Colorbar label text.
        output_path: If provided, save figure to this path.

    Returns:
        The matplotlib Figure object.
    """
    fig, ax = plt.subplots(
        figsize=(16, 8),
        subplot_kw={"projection": ccrs.Robinson()},
    )

    ax.add_feature(cfeature.OCEAN, facecolor="#e8f4f8", edgecolor="none")
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", edgecolor="none")
    ax.add_feature(cfeature.LAKES, facecolor="#d4e6f1", edgecolor="#666666", linewidth=0.2)
    ax.set_global()

    valid = values[~np.isnan(values)]
    if len(valid) == 0:
        ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")
        ax.set_title(title, fontsize=14)
        return fig

    _vmin = vmin if vmin is not None else valid[valid > 0].min() if (valid > 0).any() else 0.1
    _vmax = vmax if vmax is not None else valid.max()

    if log_scale and _vmin > 0:
        norm = mcolors.LogNorm(vmin=_vmin, vmax=_vmax)
    else:
        norm = mcolors.Normalize(vmin=_vmin, vmax=_vmax)

    mesh = ax.pcolormesh(
        lons,
        lats,
        values,
        transform=ccrs.PlateCarree(),
        norm=norm,
        cmap=cmap,
        shading="auto",
    )

    ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")

    cbar = fig.colorbar(mesh, ax=ax, orientation="horizontal", pad=0.05, shrink=0.6, aspect=30)
    cbar.set_label(cbar_label, fontsize=10)

    ax.set_title(title, fontsize=14)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        log.info("Saved figure to %s", output_path)

    return fig
