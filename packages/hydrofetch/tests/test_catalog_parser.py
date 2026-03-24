"""Tests for the catalog JSON parser."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydrofetch.catalog.parser import BandSpec, ImageExportSpec, bundled_spec_path, load_image_spec


class TestLoadImageSpec:
    def test_load_bundled_era5_spec(self):
        path = bundled_spec_path()
        spec = load_image_spec(path)
        assert spec.spec_id == "era5_land_daily_image"
        assert spec.asset_id == "ECMWF/ERA5_LAND/DAILY_AGGR"
        assert spec.crs == "EPSG:4326"
        assert len(spec.bands) == 4
        assert spec.file_format == "GeoTIFF"

    def test_band_names(self):
        spec = load_image_spec(bundled_spec_path())
        names = spec.band_names()
        assert "temperature_2m" in names
        assert "total_precipitation_sum" in names

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_image_spec(tmp_path / "does_not_exist.json")

    def test_invalid_json_raises(self, tmp_path):
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json", encoding="utf-8")
        with pytest.raises(Exception):
            load_image_spec(bad)

    def test_minimal_valid_spec(self, tmp_path):
        data = {
            "id": "test_spec",
            "asset_id": "SOME/ASSET",
            "native_scale_m": 1000,
            "bands": [{"name": "band1"}],
        }
        p = tmp_path / "spec.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        spec = load_image_spec(p)
        assert spec.spec_id == "test_spec"
        assert len(spec.bands) == 1

    def test_invalid_file_format_raises(self, tmp_path):
        data = {
            "id": "x",
            "asset_id": "X/Y",
            "native_scale_m": 100,
            "file_format": "CSV",
            "bands": [{"name": "b1"}],
        }
        p = tmp_path / "spec.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(ValueError, match="file_format"):
            load_image_spec(p)
