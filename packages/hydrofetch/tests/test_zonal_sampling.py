"""Tests for polygon-based area-weighted zonal raster sampling."""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pytest

from hydrofetch.sample.raster import load_polygons, sample_raster_by_polygons_weighted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_geojson(path: Path, features: list[dict]) -> None:
    geojson = {"type": "FeatureCollection", "features": features}
    path.write_text(json.dumps(geojson), encoding="utf-8")


def _make_polygon_feature(hylak_id: int, minx: float, miny: float, maxx: float, maxy: float) -> dict:
    return {
        "type": "Feature",
        "properties": {"hylak_id": hylak_id},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [minx, miny],
                    [maxx, miny],
                    [maxx, maxy],
                    [minx, maxy],
                    [minx, miny],
                ]
            ],
        },
    }


def _write_geotiff(path: Path, data: np.ndarray, transform, nodata: float | None = None) -> None:
    """Write a tiny float32 GeoTIFF for testing.  data shape: (bands, rows, cols)."""
    import rasterio
    from rasterio.crs import CRS  # pylint: disable=no-name-in-module,import-error

    bands, rows, cols = data.shape
    profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "width": cols,
        "height": rows,
        "count": bands,
        "crs": CRS.from_epsg(4326),
        "transform": transform,
    }
    if nodata is not None:
        profile["nodata"] = nodata

    with rasterio.open(path, "w", **profile) as dst:
        for b in range(bands):
            dst.write(data[b].astype("float32"), b + 1)
            dst.update_tags(b + 1, DESCRIPTION=f"band_{b + 1}")


# ---------------------------------------------------------------------------
# load_polygons
# ---------------------------------------------------------------------------


class TestLoadPolygons:
    def test_loads_polygon_features(self, tmp_path):
        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, [_make_polygon_feature(1, 0, 0, 1, 1)])
        df = load_polygons(gj, "hylak_id")
        assert len(df) == 1
        assert df.iloc[0]["hylak_id"] == 1
        assert df.iloc[0]["geometry"] is not None

    def test_skips_point_features(self, tmp_path):
        gj = tmp_path / "mixed.geojson"
        point_feat = {
            "type": "Feature",
            "properties": {"hylak_id": 99},
            "geometry": {"type": "Point", "coordinates": [0.5, 0.5]},
        }
        _write_geojson(gj, [_make_polygon_feature(1, 0, 0, 1, 1), point_feat])
        df = load_polygons(gj, "hylak_id")
        assert len(df) == 1
        assert df.iloc[0]["hylak_id"] == 1

    def test_raises_on_empty(self, tmp_path):
        gj = tmp_path / "empty.geojson"
        _write_geojson(gj, [])
        with pytest.raises(ValueError, match="No Polygon"):
            load_polygons(gj)

    def test_skips_missing_id(self, tmp_path):
        gj = tmp_path / "no_id.geojson"
        feat = {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            },
        }
        _write_geojson(gj, [feat])
        with pytest.raises(ValueError):
            load_polygons(gj)

    def test_repairs_invalid_polygon_with_make_valid(self, tmp_path):
        gj = tmp_path / "invalid.geojson"
        # Bow-tie self-intersection: invalid polygon that should be repaired.
        feat = {
            "type": "Feature",
            "properties": {"hylak_id": 42},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]],
                ],
            },
        }
        _write_geojson(gj, [feat])

        df = load_polygons(gj)
        assert len(df) == 1
        assert df.iloc[0]["hylak_id"] == 42
        assert df.iloc[0]["geometry"].is_valid


# ---------------------------------------------------------------------------
# sample_raster_by_polygons_weighted
# ---------------------------------------------------------------------------


class TestZonalSampling:
    def test_single_pixel_lake_exact_value(self, tmp_path):
        """A polygon perfectly covering one pixel should return that pixel's value."""
        import rasterio.transform

        transform = rasterio.transform.from_bounds(0, 0, 2, 2, 2, 2)
        data = np.array([[[10.0, 20.0], [30.0, 40.0]]])  # 1 band, 2x2 raster

        tif = tmp_path / "era5.tif"
        _write_geotiff(tif, data, transform)

        # Lake covers exactly the top-left pixel (col=0, row=0 → x:[0,1], y:[1,2])
        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, [_make_polygon_feature(1, 0.0, 1.0, 1.0, 2.0)])

        df = sample_raster_by_polygons_weighted(tif, gj, "hylak_id", "2020-01-01")
        assert len(df) == 1
        assert df.iloc[0]["hylak_id"] == 1
        assert df.iloc[0]["date"] == "2020-01-01"
        assert math.isclose(df.iloc[0]["band_1"], 10.0, abs_tol=1e-5)

    def test_lake_spanning_two_pixels_weighted(self, tmp_path):
        """A lake covering 25% of pixel A and 75% of pixel B → weighted mean."""
        import rasterio.transform

        # Single row, two columns: x:[0,2], y:[0,1]
        transform = rasterio.transform.from_bounds(0, 0, 2, 1, 2, 1)
        data = np.array([[[0.0, 100.0]]])  # left pixel=0, right pixel=100

        tif = tmp_path / "era5.tif"
        _write_geotiff(tif, data, transform)

        # Lake covers x:[0.5, 2.0], y:[0, 1] → 25% of left pixel, 100% of right
        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, [_make_polygon_feature(1, 0.5, 0.0, 2.0, 1.0)])

        df = sample_raster_by_polygons_weighted(tif, gj, "hylak_id", "2020-01-01")
        # Intersection areas: left=0.5*1=0.5, right=1*1=1.0
        # Weighted mean = (0*0.5 + 100*1.0) / (0.5 + 1.0) = 66.667
        expected = (0.0 * 0.5 + 100.0 * 1.0) / 1.5
        assert math.isclose(df.iloc[0]["band_1"], expected, rel_tol=1e-4)

    def test_lake_outside_raster_returns_nan(self, tmp_path):
        import rasterio.transform

        transform = rasterio.transform.from_bounds(0, 0, 1, 1, 1, 1)
        data = np.array([[[5.0]]])

        tif = tmp_path / "era5.tif"
        _write_geotiff(tif, data, transform)

        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, [_make_polygon_feature(1, 10.0, 10.0, 11.0, 11.0)])

        df = sample_raster_by_polygons_weighted(tif, gj, "hylak_id", "2020-01-01")
        assert math.isnan(df.iloc[0]["band_1"])

    def test_nodata_pixels_excluded_from_mean(self, tmp_path):
        """Nodata pixels must not contribute to the weighted mean."""
        import rasterio.transform

        nodata_val = -9999.0
        transform = rasterio.transform.from_bounds(0, 0, 2, 1, 2, 1)
        data = np.array([[[nodata_val, 50.0]]])

        tif = tmp_path / "era5.tif"
        _write_geotiff(tif, data, transform, nodata=nodata_val)

        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, [_make_polygon_feature(1, 0.0, 0.0, 2.0, 1.0)])

        df = sample_raster_by_polygons_weighted(tif, gj, "hylak_id", "2020-01-01")
        # Only the right pixel (50.0) contributes; left is nodata.
        assert math.isclose(df.iloc[0]["band_1"], 50.0, abs_tol=1e-5)

    def test_multiple_lakes_and_bands(self, tmp_path):
        """Multiple lakes and two bands are all correctly sampled."""
        import rasterio.transform

        transform = rasterio.transform.from_bounds(0, 0, 2, 2, 2, 2)
        band1 = np.array([[10.0, 20.0], [30.0, 40.0]])
        band2 = np.array([[1.0, 2.0], [3.0, 4.0]])
        data = np.stack([band1, band2], axis=0)

        tif = tmp_path / "era5.tif"
        _write_geotiff(tif, data, transform)

        feats = [
            _make_polygon_feature(1, 0.0, 1.0, 1.0, 2.0),  # top-left pixel
            _make_polygon_feature(2, 1.0, 0.0, 2.0, 1.0),  # bottom-right pixel
        ]
        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, feats)

        df = sample_raster_by_polygons_weighted(tif, gj, "hylak_id", "2020-01-15")
        df = df.set_index("hylak_id")
        assert math.isclose(df.loc[1, "band_1"], 10.0, abs_tol=1e-5)
        assert math.isclose(df.loc[1, "band_2"], 1.0, abs_tol=1e-5)
        assert math.isclose(df.loc[2, "band_1"], 40.0, abs_tol=1e-5)
        assert math.isclose(df.loc[2, "band_2"], 4.0, abs_tol=1e-5)
