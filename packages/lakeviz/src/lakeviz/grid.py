"""Grid aggregation utilities for global lake maps.

Provides two approaches:
  - ``build_grid_counts`` / ``build_grid_stats``: Python-side aggregation
    from raw event DataFrames (kept for backward compatibility).
  - ``agg_to_grid_matrix``: Convert SQL-side aggregated results into a
    2D numpy matrix suitable for ``pcolormesh`` rendering.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon


def build_grid_counts(
    df: pd.DataFrame,
    resolution: float = 0.5,
) -> gpd.GeoDataFrame:
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


def agg_to_grid_matrix(
    agg_df: pd.DataFrame,
    value_col: str,
    resolution: float = 0.5,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Convert SQL-side aggregated results to a 2D grid matrix for pcolormesh.

    Args:
        agg_df: DataFrame with columns [cell_lat, cell_lon, value_col].
            Produced by ``fetch_*_grid_agg`` functions in lakesource readers.
        value_col: Column to map into the grid matrix.
        resolution: Grid cell size in degrees.

    Returns:
        Tuple of (lons, lats, values) where:
        - lons: 1D array of cell-center longitudes (n_lon,)
        - lats: 1D array of cell-center latitudes (n_lat,)
        - values: 2D array of shape (n_lat, n_lon), NaN where no data.
    """
    n_lon = int(360 / resolution)
    n_lat = int(180 / resolution)

    lons = np.linspace(-180 + resolution / 2, 180 - resolution / 2, n_lon)
    lats = np.linspace(-90 + resolution / 2, 90 - resolution / 2, n_lat)

    values = np.full((n_lat, n_lon), np.nan, dtype=float)

    if agg_df.empty or value_col not in agg_df.columns:
        return lons, lats, values

    for _, row in agg_df.iterrows():
        lat_val = float(row["cell_lat"])
        lon_val = float(row["cell_lon"])
        val = float(row[value_col])

        lat_idx = int(round((lat_val + 90) / resolution))
        lon_idx = int(round((lon_val + 180) / resolution))

        if 0 <= lat_idx < n_lat and 0 <= lon_idx < n_lon:
            values[lat_idx, lon_idx] = val

    return lons, lats, values
