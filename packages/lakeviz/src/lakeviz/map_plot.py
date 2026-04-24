"""Cartopy-based global distribution maps for lake events."""

from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import cartopy.crs as ccrs
import cartopy.feature as cfeature

log = logging.getLogger(__name__)


def plot_global_grid(
    grid_gdf: gpd.GeoDataFrame,
    value_col: str = "mean_per_lake",
    title: str = "",
    cmap: str = "YlOrRd",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    output_path: Path | None = None,
) -> plt.Figure:
    """Plot a global grid map using Robinson projection.

    Args:
        grid_gdf: GeoDataFrame from build_grid_counts with geometry and value columns.
        value_col: Column name to visualize (default 'mean_per_lake').
        title: Figure title.
        cmap: Matplotlib colormap name.
        log_scale: Whether to use logarithmic color scale.
        vmin: Minimum value for color scale (auto if None).
        vmax: Maximum value for color scale (auto if None).
        output_path: If provided, save figure to this path.

    Returns:
        The matplotlib Figure object.
    """
    fig, ax = plt.subplots(
        figsize=(16, 8),
        subplot_kw={"projection": ccrs.Robinson()},
    )

    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", edgecolor="none")
    ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")
    ax.add_feature(cfeature.LAKES, facecolor="#d4e6f1", edgecolor="#666666", linewidth=0.2)
    ax.set_global()

    if grid_gdf.empty or value_col not in grid_gdf.columns:
        ax.set_title(title, fontsize=14)
        return fig

    plot_gdf = grid_gdf.dropna(subset=[value_col])
    if plot_gdf.empty:
        ax.set_title(title, fontsize=14)
        return fig

    data = plot_gdf[value_col].to_numpy()
    _vmin = vmin if vmin is not None else data[data > 0].min() if (data > 0).any() else 0.1
    _vmax = vmax if vmax is not None else data.max()

    if log_scale and _vmin > 0:
        norm = mcolors.LogNorm(vmin=_vmin, vmax=_vmax)
    else:
        norm = mcolors.Normalize(vmin=_vmin, vmax=_vmax)

    plot_gdf.to_crs(ccrs.Robinson().proj4_init).plot(
        column=value_col,
        ax=ax,
        norm=norm,
        cmap=cmap,
        edgecolor="none",
        linewidth=0,
        transform=ccrs.PlateCarree(),
    )

    sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, orientation="horizontal", pad=0.05, shrink=0.6, aspect=30)
    cbar.set_label("Events per lake" if value_col == "mean_per_lake" else value_col, fontsize=10)

    ax.set_title(title, fontsize=14)

    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(output_path, dpi=300, bbox_inches="tight")
        log.info("Saved figure to %s", output_path)

    return fig
