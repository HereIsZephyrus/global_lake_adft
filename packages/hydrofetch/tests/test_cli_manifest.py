"""Tests for the tile-manifest CLI expansion and legacy single-tile mode."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydrofetch.cli import main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_geojson(path: Path, obj: dict) -> None:
    path.write_text(json.dumps(obj), encoding="utf-8")


def _simple_polygon_geojson() -> dict:
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"hylak_id": 1},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
                },
            }
        ],
    }


def _simple_region_geojson() -> dict:
    return {
        "type": "Feature",
        "properties": {},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]]],
        },
    }


# ---------------------------------------------------------------------------
# Tile-manifest mode
# ---------------------------------------------------------------------------


class TestTileManifestEnqueue:
    def test_two_tiles_times_three_days(self, tmp_path):
        """With 2 tiles and 3 dates (exclusive end) -> 6 jobs enqueued."""
        tile_dir = tmp_path / "tiles"
        tile_dir.mkdir()
        for name in ("europe", "africa"):
            _write_geojson(tile_dir / f"{name}_lakes.geojson", _simple_polygon_geojson())
            _write_geojson(tile_dir / f"{name}_region.geojson", _simple_region_geojson())

        manifest = {
            "tiles": [
                {
                    "tile_id": "europe",
                    "region_path": "europe_region.geojson",
                    "geometry_path": "europe_lakes.geojson",
                },
                {
                    "tile_id": "africa",
                    "region_path": "africa_region.geojson",
                    "geometry_path": "africa_lakes.geojson",
                },
            ]
        }
        manifest_path = tile_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        # --job-dir is a top-level argument; must appear before the subcommand.
        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-01-01",
            "--end", "2020-01-04",
            "--tile-manifest", str(manifest_path),
            "--sink", "db",
        ])

        job_files = list(job_dir.glob("*.json"))
        assert len(job_files) == 6

    def test_job_id_contains_tile_id(self, tmp_path):
        tile_dir = tmp_path / "tiles"
        tile_dir.mkdir()
        _write_geojson(tile_dir / "eu_lakes.geojson", _simple_polygon_geojson())
        manifest = {
            "tiles": [
                {"tile_id": "europe", "geometry_path": "eu_lakes.geojson"},
            ]
        }
        (tile_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-06-01",
            "--end", "2020-06-02",
            "--tile-manifest", str(tile_dir / "manifest.json"),
            "--sink", "db",
        ])

        job_files = list(job_dir.glob("*.json"))
        assert len(job_files) == 1
        assert "europe" in job_files[0].stem

    def test_tile_without_region_creates_null_region_geojson(self, tmp_path):
        """A tile entry without region_path -> region_geojson=None in the job spec."""
        tile_dir = tmp_path / "tiles"
        tile_dir.mkdir()
        _write_geojson(tile_dir / "lakes.geojson", _simple_polygon_geojson())
        manifest = {"tiles": [{"tile_id": "global", "geometry_path": "lakes.geojson"}]}
        (tile_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-01-01",
            "--end", "2020-01-02",
            "--tile-manifest", str(tile_dir / "manifest.json"),
            "--sink", "db",
        ])

        import json as _json
        record_dict = _json.loads(next(job_dir.glob("*.json")).read_text())
        assert record_dict["spec"]["gee"]["region_geojson"] is None

    def test_dry_run_does_not_create_files(self, tmp_path, capsys):
        tile_dir = tmp_path / "tiles"
        tile_dir.mkdir()
        _write_geojson(tile_dir / "lakes.geojson", _simple_polygon_geojson())
        manifest = {"tiles": [{"tile_id": "test", "geometry_path": "lakes.geojson"}]}
        (tile_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-01-01",
            "--end", "2020-01-03",
            "--tile-manifest", str(tile_dir / "manifest.json"),
            "--sink", "db",
            "--dry-run",
        ])

        assert list(job_dir.glob("*.json")) == []
        captured = capsys.readouterr()
        assert "[dry-run]" in captured.out
        assert "test" in captured.out


# ---------------------------------------------------------------------------
# Legacy single-tile mode
# ---------------------------------------------------------------------------


class TestLegacySingleTile:
    def test_geometry_only_creates_jobs(self, tmp_path):
        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, _simple_polygon_geojson())
        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-02-01",
            "--end", "2020-02-03",
            "--geometry", str(gj),
            "--sink", "db",
        ])

        job_files = list(job_dir.glob("*.json"))
        assert len(job_files) == 2

    def test_legacy_job_id_has_no_tile_suffix(self, tmp_path):
        """Legacy mode (--geometry without manifest) uses the old naming convention."""
        gj = tmp_path / "lakes.geojson"
        _write_geojson(gj, _simple_polygon_geojson())
        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-03-15",
            "--end", "2020-03-16",
            "--geometry", str(gj),
            "--sink", "db",
        ])

        job_files = list(job_dir.glob("*.json"))
        assert len(job_files) == 1
        # Legacy job id ends with the date, no tile suffix.
        assert job_files[0].stem == "era5_land_daily_image_20200315"

    def test_geometry_and_region_creates_job_with_region(self, tmp_path):
        gj = tmp_path / "lakes.geojson"
        rj = tmp_path / "region.geojson"
        _write_geojson(gj, _simple_polygon_geojson())
        _write_geojson(rj, _simple_region_geojson())
        job_dir = tmp_path / "jobs"
        job_dir.mkdir()

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-04-01",
            "--end", "2020-04-02",
            "--geometry", str(gj),
            "--region", str(rj),
            "--sink", "db",
        ])

        import json as _json
        record_dict = _json.loads(next(job_dir.glob("*.json")).read_text())
        assert record_dict["spec"]["gee"]["region_geojson"] is not None

    def test_geometry_and_tile_manifest_mutually_exclusive(self, tmp_path):
        gj = tmp_path / "lakes.geojson"
        mf = tmp_path / "manifest.json"
        gj.write_text("{}", encoding="utf-8")
        mf.write_text("{}", encoding="utf-8")

        with pytest.raises(SystemExit):
            main([
                "era5",
                "--start", "2020-01-01",
                "--end", "2020-01-02",
                "--geometry", str(gj),
                "--tile-manifest", str(mf),
                "--sink", "db",
            ])
