"""Cartopy-based global distribution maps using pcolormesh.

``draw_global_grid`` operates on a single Axes (geographic projection).
``plot_global_grid`` is the backward-compatible convenience wrapper.

Colorbar style follows NCL conventions (vertical, drawedges, extendrect,
extendfrac='auto', manual ticks, labelsize=10).

When ``add_cbar=False``, the function returns a dict with ``mesh``, ``norm``,
``bounds``, ``vmin``, ``vmax``, and ``log_scale`` so that callers can build
shared colorbars in panel layouts.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.ticker as mticker
import cartopy.crs as ccrs
import cartopy.feature as cfeature

from lakeviz.config import DEFAULT_VIZ_CONFIG
from lakeviz.style.base import AxKind, stamp_ax
from lakeviz.style.presets import resolve_cmap

log = logging.getLogger(__name__)


def _discrete_norm(
    vmin: float, vmax: float, n_levels: int, log_scale: bool,
) -> tuple[mcolors.BoundaryNorm, np.ndarray]:
    if log_scale and vmin > 0:
        bounds = np.logspace(
            np.log10(vmin), np.log10(vmax), n_levels + 1,
        )
    else:
        bounds = np.linspace(vmin, vmax, n_levels + 1)
    norm = mcolors.BoundaryNorm(bounds, ncolors=n_levels)
    return norm, bounds


def draw_global_grid(
    ax: plt.Axes,
    lons: np.ndarray,
    lats: np.ndarray,
    values: np.ndarray,
    *,
    title: str = "",
    cmap: str = "sequential_warm",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    cbar_label: str = "",
    cbar_orientation: str = "vertical",
    n_levels: int = 5,
    add_cbar: bool = True,
) -> dict[str, Any] | None:
    """Draw a global grid map on *ax* using pcolormesh (NCL-style colorbar).

    The Axes must already have a Cartopy projection (e.g. Robinson).
    This function stamps ``ax._ax_kind = AxKind.GEOGRAPHIC``.

    When ``add_cbar=False``, no colorbar is drawn and a dict with keys
    ``mesh``, ``norm``, ``bounds``, ``vmin``, ``vmax``, ``log_scale`` is
    returned for external colorbar composition.
    """
    stamp_ax(ax, AxKind.GEOGRAPHIC)

    resolved_cmap = resolve_cmap(cmap)

    ax.add_feature(cfeature.OCEAN, facecolor="#e8f4f8", edgecolor="none")
    ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", edgecolor="none")
    ax.add_feature(cfeature.LAKES, facecolor="#d4e6f1", edgecolor="#666666", linewidth=0.2)
    ax.set_global()

    valid = values[~np.isnan(values)]
    if len(valid) == 0:
        ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")
        if title:
            ax.set_title(title, fontsize=14)
        return None

    _vmin = (
        vmin if vmin is not None
        else float(valid[valid > 0].min()) if (valid > 0).any() else 0.1
    )
    _vmax = vmax if vmax is not None else float(valid.max())

    norm, bounds = _discrete_norm(_vmin, _vmax, n_levels, log_scale)

    mesh = ax.pcolormesh(
        lons, lats, values,
        transform=ccrs.PlateCarree(),
        norm=norm,
        cmap=resolved_cmap,
        shading="auto",
    )

    ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")

    if title:
        ax.set_title(title, fontsize=14)

    meta: dict[str, Any] = {
        "mesh": mesh,
        "norm": norm,
        "bounds": bounds,
        "vmin": _vmin,
        "vmax": _vmax,
        "log_scale": log_scale,
    }

    if not add_cbar:
        return meta

    ticks = bounds

    cbar_kwargs = {
        "orientation": cbar_orientation,
        "shrink": 0.8,
        "extendrect": True,
        "extendfrac": "auto",
        "drawedges": True,
        "ticks": ticks,
    }
    if cbar_orientation == "vertical":
        cbar_kwargs["pad"] = 0.05
    else:
        cbar_kwargs["pad"] = 0.11
        cbar_kwargs["aspect"] = 30

    fig = ax.get_figure()
    cbar = fig.colorbar(mesh, ax=ax, **cbar_kwargs)
    cbar.ax.tick_params(labelsize=10)

    if cbar_orientation == "vertical":
        cbar.ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:.2g}"),
        )
    else:
        cbar.ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, _: f"{x:.2g}"),
        )

    if cbar_label:
        if cbar_orientation == "vertical":
            cbar.ax.set_ylabel(cbar_label, fontsize=10)
        else:
            cbar.ax.set_xlabel(cbar_label, fontsize=10)

    return meta


def plot_global_grid(
    lons: np.ndarray,
    lats: np.ndarray,
    values: np.ndarray,
    title: str = "",
    cmap: str = "sequential_warm",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    cbar_label: str = "",
    output_path: Path | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(
        figsize=(14, 7),
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


def draw_global_density(
    ax: plt.Axes,
    lons: np.ndarray,
    lats: np.ndarray,
    values: np.ndarray,
    *,
    title: str = "",
    cmap: str = "sequential_warm",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    cbar_label: str = "",
    cbar_orientation: str = "vertical",
    n_levels: int = 5,
    sigma: float = 1.0,
    target_res: float = 0.1,
    add_cbar: bool = True,
) -> dict[str, Any] | None:
    """Draw a Gaussian-smoothed global density map on *ax*.

    Takes a coarse-resolution (e.g. 0.5°) grid matrix, applies Gaussian
    smoothing, upsamples to *target_res* via bicubic interpolation, and
    renders with :func:`draw_global_grid`.

    Parameters match :func:`draw_global_grid` with two additions:

    sigma:
        Gaussian filter standard deviation in units of input grid cells.
    target_res:
        Output resolution in degrees for the rendered grid.
    """
    from scipy.ndimage import gaussian_filter, zoom

    filled = np.nan_to_num(values, nan=0.0)
    if not np.any(filled > 0):
        stamp_ax(ax, AxKind.GEOGRAPHIC)
        ax.add_feature(cfeature.OCEAN, facecolor="#e8f4f8", edgecolor="none")
        ax.add_feature(cfeature.LAND, facecolor="#f0f0f0", edgecolor="none")
        ax.add_feature(cfeature.LAKES, facecolor="#d4e6f1", edgecolor="#666666", linewidth=0.2)
        ax.set_global()
        ax.add_feature(cfeature.COASTLINE, linewidth=0.3, color="#666666")
        if title:
            ax.set_title(title, fontsize=14)
        return None

    smoothed = gaussian_filter(filled, sigma=sigma, mode="constant", cval=0.0)

    scale = float(lons[1] - lons[0]) / target_res
    upsampled = zoom(smoothed, scale, order=3)

    threshold = np.nanmax(upsampled) * 0.005
    mask = upsampled < threshold
    if log_scale and mask.any():
        upsampled = upsampled.copy()
        upsampled[mask] = np.nan

    n_lon_up = upsampled.shape[1]
    n_lat_up = upsampled.shape[0]
    lons_up = np.linspace(-180 + target_res / 2, 180 - target_res / 2, n_lon_up)
    lats_up = np.linspace(-90 + target_res / 2, 90 - target_res / 2, n_lat_up)

    return draw_global_grid(
        ax, lons_up, lats_up, upsampled,
        title=title, cmap=cmap, log_scale=log_scale,
        vmin=vmin, vmax=vmax, cbar_label=cbar_label,
        cbar_orientation=cbar_orientation, n_levels=n_levels,
        add_cbar=add_cbar,
    )