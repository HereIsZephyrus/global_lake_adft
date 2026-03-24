"""Polygon-based zonal raster sampling for lake forcing values.

Reads a multi-band GeoTIFF and a lake polygon GeoJSON file and returns a
:class:`~pandas.DataFrame` with one row per lake containing the
**area-weighted mean** pixel value for each band.

For each lake polygon the algorithm:

1. Derives the pixel window that covers the polygon bounding box.
2. For every pixel in that window computes the intersection area between
   the pixel box and the lake polygon (using Shapely).
3. Returns ``sum(pixel_value * intersection_area) / sum(intersection_area)``
   for each band as the representative lake forcing value.

This is more robust than centroid sampling for ERA5-Land (≈ 0.1° / 11 km
pixels) where many lakes are smaller than a single pixel, and avoids the
instability of centroid-to-pixel snapping when centroids sit near pixel
edges.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Geometry loading
# ---------------------------------------------------------------------------


def load_polygons(
    path: Path,
    id_column: str = "hylak_id",
) -> pd.DataFrame:
    """Return a DataFrame with ``[id_column, geometry]`` from a GeoJSON file.

    Only Polygon and MultiPolygon features are loaded; Point features are
    silently skipped.

    Args:
        path: Path to a GeoJSON FeatureCollection.
        id_column: Name of the lake identifier property in each Feature.

    Returns:
        DataFrame with columns ``[id_column, "geometry"]`` where ``geometry``
        is a :class:`shapely.geometry.base.BaseGeometry`.

    Raises:
        ValueError: If the file contains no eligible features.
    """
    from shapely.geometry import shape  # pylint: disable=import-outside-toplevel

    with path.open(encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)

    features = data.get("features", [])
    rows: list[dict] = []
    for feat in features:
        geom_data = feat.get("geometry") or {}
        props = feat.get("properties") or {}
        geom_type = geom_data.get("type", "")
        if geom_type not in ("Polygon", "MultiPolygon"):
            continue
        lid = props.get(id_column)
        if lid is None:
            continue
        rows.append({id_column: lid, "geometry": shape(geom_data)})

    df = pd.DataFrame(rows)
    if df.empty:
        raise ValueError(
            f"No Polygon/MultiPolygon features with property {id_column!r} found in {path}"
        )
    return df


# ---------------------------------------------------------------------------
# Zonal sampling
# ---------------------------------------------------------------------------


def sample_raster_by_polygons_weighted(
    raster_path: Path,
    geometry_path: Path,
    id_column: str,
    date_iso: str,
) -> pd.DataFrame:
    """Sample a multi-band GeoTIFF by lake polygon using area-weighted mean.

    For each lake polygon the pixel window that covers the polygon bounding
    box is extracted.  Each pixel in the window is weighted by its intersection
    area with the lake polygon.  The result is the area-weighted mean of valid
    (non-nodata) pixels for every band.

    Lakes whose bounding box falls entirely outside the raster or whose
    polygon intersects no raster pixels (zero total weight) return ``NaN``
    for all bands.

    Args:
        raster_path: Path to a single-date ERA5 GeoTIFF (one band per variable).
        geometry_path: Path to a GeoJSON FeatureCollection with lake
            Polygon / MultiPolygon features.
        id_column: Name of the lake identifier property in the GeoJSON.
        date_iso: ISO-8601 date string for the ``date`` column in the output.

    Returns:
        DataFrame with columns ``[id_column, "date", <band_name>, ...]``,
        one row per lake.
    """
    import rasterio  # pylint: disable=import-outside-toplevel
    import rasterio.windows  # pylint: disable=import-outside-toplevel
    from shapely.geometry import box  # pylint: disable=import-outside-toplevel

    polygons = load_polygons(geometry_path, id_column)

    records: list[dict] = []

    with rasterio.open(raster_path) as src:
        band_names: list[str] = (
            list(src.descriptions) if src.descriptions else [f"band_{i + 1}" for i in range(src.count)]
        )
        nodata = src.nodata
        raster_bounds = src.bounds  # BoundingBox(left, bottom, right, top)

        for _, row in polygons.iterrows():
            lake_id = row[id_column]
            geom = row["geometry"]

            entry: dict = {id_column: lake_id, "date": date_iso}

            # Clip polygon bounds to raster extent for the window query.
            minx, miny, maxx, maxy = geom.bounds
            if (
                minx >= raster_bounds.right
                or maxx <= raster_bounds.left
                or miny >= raster_bounds.top
                or maxy <= raster_bounds.bottom
            ):
                # Polygon entirely outside raster.
                for name in band_names:
                    entry[name] = np.nan
                records.append(entry)
                continue

            # Build the pixel window for the polygon bounding box.
            raw_window = rasterio.windows.from_bounds(
                minx, miny, maxx, maxy, transform=src.transform
            )
            # Round to whole pixels and clamp to raster extent.
            window = raw_window.round_lengths().round_offsets()
            window = window.intersection(
                rasterio.windows.Window(0, 0, src.width, src.height)
            )

            win_w = int(window.width)
            win_h = int(window.height)
            if win_w <= 0 or win_h <= 0:
                for name in band_names:
                    entry[name] = np.nan
                records.append(entry)
                continue

            # Read all bands for the window at once: shape (bands, rows, cols).
            data = src.read(window=window).astype(float)
            if nodata is not None:
                data[data == nodata] = np.nan

            win_transform = src.window_transform(window)

            # Build intersection-area weight matrix.
            weights = _compute_weights(win_transform, win_h, win_w, geom, box)

            total_weight = float(weights.sum())
            if total_weight == 0.0:
                for name in band_names:
                    entry[name] = np.nan
            else:
                for band_idx, name in enumerate(band_names):
                    band_data = data[band_idx]
                    valid = ~np.isnan(band_data)
                    w = weights * valid
                    w_sum = float(w.sum())
                    if w_sum == 0.0:
                        entry[name] = np.nan
                    else:
                        entry[name] = float(np.sum(band_data * w) / w_sum)

            records.append(entry)

    result = pd.DataFrame(records)
    log.debug(
        "Zonal sampling: %d lakes from %s for date %s",
        len(result),
        raster_path.name,
        date_iso,
    )
    return result


def _compute_weights(
    win_transform,
    nrows: int,
    ncols: int,
    geom,
    box_fn,
) -> np.ndarray:
    """Return an (nrows, ncols) array of intersection areas between pixels and *geom*.

    Uses the window's affine transform to construct each pixel's bounding box,
    then calls :func:`shapely.geometry.base.BaseGeometry.intersection`.

    ERA5-Land pixels are large (≈ 0.1° / 11 km), so most lake windows contain
    only a handful of pixels; the Python loop is fast enough in practice.
    """
    a = win_transform.a   # pixel width (positive)
    e = win_transform.e   # pixel height (negative for north-up rasters)
    c = win_transform.c   # x of top-left pixel corner
    f = win_transform.f   # y of top-left pixel corner

    weights = np.zeros((nrows, ncols), dtype=float)
    for r in range(nrows):
        for col in range(ncols):
            x0 = c + col * a
            x1 = x0 + a
            y0 = f + r * e          # top edge of pixel  (larger y for north-up)
            y1 = y0 + e             # bottom edge        (smaller y)
            pixel_box = box_fn(min(x0, x1), min(y0, y1), max(x0, x1), max(y0, y1))
            intersect = geom.intersection(pixel_box)
            weights[r, col] = intersect.area
    return weights


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


__all__ = [
    "load_polygons",
    "sample_raster_by_polygons_weighted",
]
