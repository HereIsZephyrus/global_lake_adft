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
    def test_groups_lakes_into_multiple_tiles(self):
        module = _load_module()
        lake_geometries = [
            (101, box(-101.0, 50.0, -100.0, 51.0)),
            (202, box(10.0, 45.0, 11.0, 46.0)),
            (303, box(100.0, 40.0, 101.0, 41.0)),
        ]

        grouped = module._group_lakes_by_tile(lake_geometries)

        assert list(grouped) == ["north_america", "europe", "asia"]
        assert [hid for hid, _ in grouped["north_america"]] == [101]
        assert [hid for hid, _ in grouped["europe"]] == [202]
        assert [hid for hid, _ in grouped["asia"]] == [303]

    def test_region_is_derived_per_tile_from_same_lake_geometry_set(self):
        module = _load_module()
        tile_lakes = [
            (101, box(0.0, 0.0, 1.0, 1.0)),
            (202, box(2.0, 3.0, 4.0, 5.0)),
        ]

        lakes_payload = module._lake_feature_collection(tile_lakes)
        region_feature = module._derived_region_feature(
            tile_lakes,
            buffer_deg=0.25,
            tile_id="europe",
        )

        assert [feat["properties"]["hylak_id"] for feat in lakes_payload["features"]] == [101, 202]
        assert region_feature["type"] == "Feature"
        assert region_feature["properties"]["tile_id"] == "europe"
        assert shape(region_feature["geometry"]).bounds == (-0.25, -0.25, 4.25, 5.25)

    def test_manifest_uses_relative_paths_to_generated_tile_artifacts(self, tmp_path):
        module = _load_module()
        fixtures_dir = tmp_path / "fixtures"
        fixtures_dir.mkdir()
        tiles_dir = fixtures_dir / "tiles"
        grouped_lakes = {
            "north_america": [(101, box(-101.0, 50.0, -100.0, 51.0))],
            "europe": [(202, box(10.0, 45.0, 11.0, 46.0))],
        }

        manifest = module._tile_manifest(
            manifest_path=fixtures_dir / "smoke_manifest.json",
            tiles_dir=tiles_dir,
            grouped_lakes=grouped_lakes,
        )

        assert manifest == {
            "tiles": [
                {
                    "tile_id": "north_america",
                    "region_path": "tiles/north_america_region.geojson",
                    "geometry_path": "tiles/north_america_lakes.geojson",
                },
                {
                    "tile_id": "europe",
                    "region_path": "tiles/europe_region.geojson",
                    "geometry_path": "tiles/europe_lakes.geojson",
                }
            ]
        }
