"""Tests for smoke fixture generation helpers."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from shapely.geometry import box, shape


def _load_module():
    script_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "smoke"
        / "generate_smoke_fixtures.py"
    )
    spec = importlib.util.spec_from_file_location("generate_smoke_fixtures", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class TestSmokeFixtureHelpers:
    # pylint: disable=protected-access
    def test_region_is_derived_from_same_lake_geometry_set(self):
        module = _load_module()
        lake_geometries = [
            (101, box(0.0, 0.0, 1.0, 1.0)),
            (202, box(2.0, 3.0, 4.0, 5.0)),
        ]

        lakes_payload = module._lake_feature_collection(lake_geometries)
        region_feature = module._derived_region_feature(
            lake_geometries,
            buffer_deg=0.25,
            tile_id="smoke",
        )

        assert [feat["properties"]["hylak_id"] for feat in lakes_payload["features"]] == [101, 202]
        assert region_feature["type"] == "Feature"
        assert region_feature["properties"]["tile_id"] == "smoke"
        assert shape(region_feature["geometry"]).bounds == (-0.25, -0.25, 4.25, 5.25)

    def test_manifest_uses_relative_paths_to_generated_artifacts(self, tmp_path):
        module = _load_module()
        fixtures_dir = tmp_path / "fixtures"
        fixtures_dir.mkdir()

        manifest = module._tile_manifest(
            manifest_path=fixtures_dir / "smoke_manifest.json",
            geometry_path=fixtures_dir / "smoke_lakes_polygons.geojson",
            region_path=fixtures_dir / "smoke_region.geojson",
            tile_id="smoke",
        )

        assert manifest == {
            "tiles": [
                {
                    "tile_id": "smoke",
                    "region_path": "smoke_region.geojson",
                    "geometry_path": "smoke_lakes_polygons.geojson",
                }
            ]
        }
