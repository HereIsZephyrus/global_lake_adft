"""0.5-degree grid binning and aggregation for global lake event maps."""

from __future__ import annotations

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon


def build_grid_counts(
    df: pd.DataFrame,
    resolution: float = 0.5,
) -> gpd.GeoDataFrame:
    """Bin lake events into a regular lat/lon grid and compute per-cell statistics.

    Each grid cell contains:
    - lake_count: number of unique lakes in the cell
    - event_count: total number of events in the cell
    - mean_per_lake: event_count / lake_count (average events per lake)

    Args:
        df: DataFrame with columns [hylak_id, lat, lon].
            Additional columns (e.g. event_type, transition_type) are preserved
            but not used in aggregation.
        resolution: Grid cell size in degrees (default 0.5).

    Returns:
        GeoDataFrame with columns [geometry, lake_count, event_count, mean_per_lake].
        Only cells with at least one lake are included.
    """
    if df.empty:
        return gpd.GeoDataFrame(
            columns=["geometry", "lake_count", "event_count", "mean_per_lake"],
            crs="EPSG:4326",
        )

    lon_bins = np.arange(-180, 180 + resolution, resolution)
    lat_bins = np.arange(-90, 90 + resolution, resolution)

    lon_idx = np.digitize(df["lon"].to_numpy(), lon_bins) - 1
    lat_idx = np.digitize(df["lat"].to_numpy(), lat_bins) - 1

    lon_idx = np.clip(lon_idx, 0, len(lon_bins) - 2)
    lat_idx = np.clip(lat_idx, 0, len(lat_bins) - 2)

    grid_df = df.copy()
    grid_df["_lon_idx"] = lon_idx
    grid_df["_lat_idx"] = lat_idx
    grid_df["_cell_key"] = lon_idx * 10000 + lat_idx

    grouped = grid_df.groupby("_cell_key")

    rows = []
    for _cell_key, group in grouped:
        li = group["_lat_idx"].iloc[0]
        lo = group["_lon_idx"].iloc[0]

        lon_min = lon_bins[lo]
        lon_max = lon_bins[lo + 1]
        lat_min = lat_bins[li]
        lat_max = lat_bins[li + 1]

        polygon = Polygon([
            (lon_min, lat_min),
            (lon_max, lat_min),
            (lon_max, lat_max),
            (lon_min, lat_max),
        ])

        lake_count = group["hylak_id"].nunique()
        event_count = len(group)
        mean_per_lake = event_count / lake_count if lake_count > 0 else 0.0

        rows.append({
            "geometry": polygon,
            "lake_count": lake_count,
            "event_count": event_count,
            "mean_per_lake": mean_per_lake,
        })

    if not rows:
        return gpd.GeoDataFrame(
            columns=["geometry", "lake_count", "event_count", "mean_per_lake"],
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(rows, crs="EPSG:4326")


_AGG_FUNCS = {
    "median": np.median,
    "mean": np.mean,
    "sum": np.sum,
    "std": np.std,
}


def build_grid_stats(
    df: pd.DataFrame,
    agg_specs: dict[str, tuple[str, str]],
    resolution: float = 0.5,
) -> gpd.GeoDataFrame:
    """Bin lake data into a regular lat/lon grid with custom aggregation.

    Args:
        df: DataFrame with columns [hylak_id, lat, lon] plus value columns.
        agg_specs: Mapping of output column name to (source_column, agg_method).
            agg_method is one of "median", "mean", "sum", "std".
            Example: {"median_xi": ("xi", "median"), "conv_rate": ("converged", "mean")}
        resolution: Grid cell size in degrees (default 0.5).

    Returns:
        GeoDataFrame with columns [geometry, lake_count] + agg_specs keys.
        Only cells with at least one lake are included.
    """
    output_cols = list(agg_specs.keys())

    if df.empty:
        return gpd.GeoDataFrame(
            columns=["geometry", "lake_count"] + output_cols,
            crs="EPSG:4326",
        )

    lon_bins = np.arange(-180, 180 + resolution, resolution)
    lat_bins = np.arange(-90, 90 + resolution, resolution)

    lon_idx = np.digitize(df["lon"].to_numpy(), lon_bins) - 1
    lat_idx = np.digitize(df["lat"].to_numpy(), lat_bins) - 1

    lon_idx = np.clip(lon_idx, 0, len(lon_bins) - 2)
    lat_idx = np.clip(lat_idx, 0, len(lat_bins) - 2)

    grid_df = df.copy()
    grid_df["_lon_idx"] = lon_idx
    grid_df["_lat_idx"] = lat_idx
    grid_df["_cell_key"] = lon_idx * 10000 + lat_idx

    grouped = grid_df.groupby("_cell_key")

    rows = []
    for _cell_key, group in grouped:
        li = group["_lat_idx"].iloc[0]
        lo = group["_lon_idx"].iloc[0]

        lon_min = lon_bins[lo]
        lon_max = lon_bins[lo + 1]
        lat_min = lat_bins[li]
        lat_max = lat_bins[li + 1]

        polygon = Polygon([
            (lon_min, lat_min),
            (lon_max, lat_min),
            (lon_max, lat_max),
            (lon_min, lat_max),
        ])

        lake_count = group["hylak_id"].nunique()

        cell: dict = {"geometry": polygon, "lake_count": lake_count}
        for out_name, (src_col, method) in agg_specs.items():
            if src_col not in group.columns:
                cell[out_name] = np.nan
                continue
            vals = group[src_col].dropna().to_numpy(dtype=float)
            if len(vals) == 0:
                cell[out_name] = np.nan
                continue
            func = _AGG_FUNCS.get(method)
            if func is None:
                raise ValueError(f"Unknown agg method {method!r}; choose from {list(_AGG_FUNCS)}")
            cell[out_name] = func(vals)

        rows.append(cell)

    if not rows:
        return gpd.GeoDataFrame(
            columns=["geometry", "lake_count"] + output_cols,
            crs="EPSG:4326",
        )

    return gpd.GeoDataFrame(rows, crs="EPSG:4326")
