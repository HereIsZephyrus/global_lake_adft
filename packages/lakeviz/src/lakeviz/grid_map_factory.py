"""Factory for global grid distribution maps — eliminates repeated boilerplate.

Each domain sub-package (quantile, eot, pwm_extreme) follows the same
pattern: fetch aggregated data via LakeProvider → filter → convert to
grid matrix → draw on a geographic ax → save.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np

from .config import GlobalGridConfig
from .grid import agg_to_grid_matrix
from .map_plot import draw_global_grid

log = logging.getLogger(__name__)


def make_grid_map(
    fetch_fn,
    value_col: str,
    *,
    title: str,
    cmap: str = "YlOrRd",
    log_scale: bool = True,
    vmin: float | None = None,
    vmax: float | None = None,
    cbar_label: str = "",
    sub_dir: str = "",
    filename: str = "map.png",
    extra_fetch_kwargs: dict | None = None,
    pre_filter_fn=None,
):
    """Return a function that generates a global grid map for a given config.

    Parameters
    ----------
    fetch_fn:
        Callable that returns a DataFrame from LakeProvider.
        Signature: ``fetch_fn(provider, resolution, *, refresh, **kwargs) -> pd.DataFrame``
    value_col:
        Column in the aggregated DataFrame to map into the grid.
    title:
        Figure title.
    cmap:
        Matplotlib colormap name.
    log_scale:
        Whether to use logarithmic color scale.
    vmin / vmax:
        Color scale bounds (auto if None).
    cbar_label:
        Colorbar label text.
    sub_dir:
        Sub-directory under ``config.output_dir`` for the output file.
    filename:
        Output file name.
    extra_fetch_kwargs:
        Additional keyword arguments passed to ``fetch_fn``.
    pre_filter_fn:
        Optional callable ``fn(agg_df) -> agg_df`` applied before grid
        conversion (e.g. filtering by event_type or transition_type).

    Returns
    -------
    A callable with signature:
        ``(config: GlobalGridConfig, *, refresh=False, min_lakes=1) -> Path``
    """
    _extra = extra_fetch_kwargs or {}

    def _grid_map_fn(
        config: GlobalGridConfig,
        *,
        refresh: bool = False,
        min_lakes: int = 1,
    ) -> Path:
        agg = fetch_fn(config.provider, config.resolution, refresh=refresh, **_extra)
        if min_lakes > 1:
            agg = agg[agg["lake_count"] >= min_lakes]
        if pre_filter_fn is not None:
            agg = pre_filter_fn(agg)

        if agg.empty:
            log.warning("No data for %s", title)
            return Path()

        if value_col == "mean_per_lake":
            agg = agg.copy()
            agg["mean_per_lake"] = agg["event_count"].astype(float) / agg["lake_count"].astype(float)

        lons, lats, values = agg_to_grid_matrix(agg, value_col, config.resolution)

        out_dir = config.output_dir / sub_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / filename

        import matplotlib.pyplot as plt
        import cartopy.crs as ccrs

        fig, ax = plt.subplots(figsize=(16, 8), subplot_kw={"projection": ccrs.Robinson()})
        draw_global_grid(
            ax, lons, lats, values,
            title=title, cmap=cmap, log_scale=log_scale,
            vmin=vmin, vmax=vmax, cbar_label=cbar_label,
        )
        fig.savefig(out_path, dpi=300, bbox_inches="tight")
        plt.close(fig)
        log.info("Saved: %s → %s", title, out_path)
        return out_path

    return _grid_map_fn