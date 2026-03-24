"""Point-based raster sampling for lake centroids.

Reads a multi-band GeoTIFF and a lake centroid file (CSV or GeoJSON) and
returns a :class:`~pandas.DataFrame` with one row per lake containing the
pixel values at the lake centroid location and a ``date`` column.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Geometry loading
# ---------------------------------------------------------------------------


def load_centroids(path: Path, id_column: str = "hylak_id") -> pd.DataFrame:
    """Return a DataFrame with ``[id_column, lon, lat]`` from *path*.

    Supports:
    - CSV with columns ``[id_column, lon, lat]``
    - GeoJSON FeatureCollection with Point geometry (id_column in properties)

    Args:
        path: Path to a CSV or ``.geojson`` / ``.json`` file.
        id_column: Name of the lake identifier column.

    Returns:
        DataFrame with columns ``[id_column, "lon", "lat"]``.

    Raises:
        ValueError: On unsupported format or missing columns.
    """
    suffix = path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(path)
        _check_columns(df, [id_column, "lon", "lat"], path)
        return df[[id_column, "lon", "lat"]].copy()

    if suffix in (".geojson", ".json"):
        import json  # pylint: disable=import-outside-toplevel

        with path.open(encoding="utf-8") as fh:
            data = json.load(fh)

        features = data.get("features", [])
        rows: list[dict] = []
        for feat in features:
            geom = feat.get("geometry", {})
            props = feat.get("properties", {})
            if geom.get("type") != "Point":
                continue
            lon, lat = geom["coordinates"][:2]
            lid = props.get(id_column)
            if lid is None:
                continue
            rows.append({id_column: lid, "lon": float(lon), "lat": float(lat)})
        df = pd.DataFrame(rows)
        if df.empty:
            raise ValueError(
                f"No Point features with property {id_column!r} found in {path}"
            )
        return df

    raise ValueError(
        f"Unsupported geometry file format {suffix!r}. Use .csv or .geojson"
    )


def _check_columns(df: pd.DataFrame, required: list[str], path: Path) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"File {path} is missing columns: {missing}")


# ---------------------------------------------------------------------------
# Raster sampling
# ---------------------------------------------------------------------------


def sample_raster_at_centroids(
    raster_path: Path,
    geometry_path: Path,
    id_column: str,
    date_iso: str,
) -> pd.DataFrame:
    """Sample a multi-band GeoTIFF at lake centroid coordinates.

    Each centroid is mapped to the raster pixel whose centre is nearest to
    the geographic location (using the raster's affine transform).  Pixels
    outside the raster extent or masked as nodata return ``NaN``.

    Args:
        raster_path: Path to a single-date ERA5 GeoTIFF (one band per variable).
        geometry_path: Path to a CSV or GeoJSON centroid file.
        id_column: Name of the lake identifier column in the centroid file.
        date_iso: ISO-8601 date string for the ``date`` column in the output.

    Returns:
        DataFrame with columns ``[id_column, "date", <band_name>, ...]``.
    """
    import rasterio  # pylint: disable=import-outside-toplevel

    centroids = load_centroids(geometry_path, id_column)

    with rasterio.open(raster_path) as src:
        band_names = src.descriptions or [f"band_{i+1}" for i in range(src.count)]
        transform = src.transform
        nodata = src.nodata

        # Read all bands at once into a (bands, rows, cols) array.
        data = src.read().astype(float)
        if nodata is not None:
            data[data == nodata] = np.nan

        nrows, ncols = src.height, src.width
        # Convert geographic coordinates to pixel indices.
        rows, cols = _lonlat_to_pixel(centroids["lon"].to_numpy(), centroids["lat"].to_numpy(), transform)

        # Clip to valid extent (out-of-range → NaN row).
        valid_mask = (rows >= 0) & (rows < nrows) & (cols >= 0) & (cols < ncols)

    records: list[dict] = []
    for idx, (row_px, col_px, is_valid) in enumerate(
        zip(rows, cols, valid_mask)
    ):
        entry: dict = {
            id_column: centroids.iloc[idx][id_column],
            "date": date_iso,
        }
        for band_idx, name in enumerate(band_names):
            if is_valid:
                entry[name] = float(data[band_idx, row_px, col_px])
            else:
                entry[name] = np.nan
        records.append(entry)

    out = pd.DataFrame(records)
    log.debug("Sampled %d lakes from %s for date %s", len(out), raster_path.name, date_iso)
    return out


def _lonlat_to_pixel(
    lons: np.ndarray,
    lats: np.ndarray,
    transform,
) -> tuple[np.ndarray, np.ndarray]:
    """Convert geographic lon/lat arrays to (row, col) integer pixel indices.

    Uses the raster's affine transform (``rasterio.transform``).

    Returns:
        Two integer arrays (rows, cols) clipped to the raster dimensions.
    """
    from rasterio.transform import rowcol  # pylint: disable=import-outside-toplevel

    rows_f, cols_f = rowcol(transform, lons, lats)
    return np.asarray(rows_f, dtype=int), np.asarray(cols_f, dtype=int)


__all__ = [
    "load_centroids",
    "sample_raster_at_centroids",
]
