"""Tests for the tile-manifest CLI expansion and legacy single-tile mode."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hydrofetch.cli import main
from hydrofetch.jobs.models import (
    GeeExportParams,
    JobRecord,
    JobSpec,
    JobState,
    SampleParams,
    WriteParams,
)
from hydrofetch.jobs.store import JobStore


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


def _make_record(job_id: str, date_iso: str = "2020-01-01") -> JobRecord:
    spec = JobSpec(
        job_id=job_id,
        export_name=job_id,
        date_iso=date_iso,
        gee=GeeExportParams(
            spec_id="era5_land_daily_image",
            asset_id="ECMWF/ERA5_LAND/DAILY_AGGR",
            bands=["temperature_2m"],
            scale=11132.0,
            crs="EPSG:4326",
            max_pixels=10**13,
            region_geojson=_simple_region_geojson(),
            tile_id="test",
        ),
        sample=SampleParams(geometry_path="/tmp/lakes.geojson", id_column="hylak_id", tile_id="test"),
        write=WriteParams(output_dir="", sinks=["db"], db_table="era5_forcing"),
    )
    return JobRecord(spec=spec)


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

        record_dict = json.loads(next(job_dir.glob("*.json")).read_text())
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

    def test_retry_failed_resets_all_failed_jobs_to_hold(self, tmp_path, capsys):
        tile_dir = tmp_path / "tiles"
        tile_dir.mkdir()
        _write_geojson(tile_dir / "lakes.geojson", _simple_polygon_geojson())
        manifest = {"tiles": [{"tile_id": "test", "geometry_path": "lakes.geojson"}]}
        manifest_path = tile_dir / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

        job_dir = tmp_path / "jobs"
        store = JobStore(job_dir)

        in_range_failed = _make_record("era5_land_daily_image_20200101_test").fail("tls")
        in_range_failed = in_range_failed.fail("tls").fail("tls")
        out_of_range_failed = _make_record("era5_land_daily_image_20191231_test", "2019-12-31").fail("tls")
        out_of_range_failed = out_of_range_failed.fail("tls").fail("tls")
        completed = _make_record("era5_land_daily_image_20200102_test", "2020-01-02").advance(
            JobState.COMPLETED
        )
        store.save(in_range_failed)
        store.save(out_of_range_failed)
        store.save(completed)

        main([
            "--job-dir", str(job_dir),
            "era5",
            "--start", "2020-01-01",
            "--end", "2020-01-02",
            "--tile-manifest", str(manifest_path),
            "--sink", "db",
            "--retry-failed",
        ])

        captured = capsys.readouterr()
        assert "Reset 2 failed job(s) to HOLD." in captured.out

        refreshed_in_range = store.load("era5_land_daily_image_20200101_test")
        refreshed_out_of_range = store.load("era5_land_daily_image_20191231_test")
        refreshed_completed = store.load("era5_land_daily_image_20200102_test")

        assert refreshed_in_range is not None
        assert refreshed_in_range.state == JobState.HOLD
        assert refreshed_in_range.attempt == 0
        assert refreshed_in_range.last_error is None

        assert refreshed_out_of_range is not None
        assert refreshed_out_of_range.state == JobState.HOLD
        assert refreshed_out_of_range.attempt == 0
        assert refreshed_out_of_range.last_error is None

        assert refreshed_completed is not None
        assert refreshed_completed.state == JobState.COMPLETED

        assert all(record.state != JobState.FAILED for record in store.load_all())


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

        record_dict = json.loads(next(job_dir.glob("*.json")).read_text())
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


class TestRetryCommand:
    def test_retry_resets_attempt_to_zero(self, tmp_path, capsys):
        job_dir = tmp_path / "jobs"
        store = JobStore(job_dir)
        failed = _make_record("era5_land_daily_image_20200103_test", "2020-01-03").fail("tls")
        failed = failed.fail("tls").fail("tls")
        store.save(failed)

        main([
            "--job-dir", str(job_dir),
            "retry",
            "--job-id", "era5_land_daily_image_20200103_test",
        ])

        captured = capsys.readouterr()
        assert "Reset job era5_land_daily_image_20200103_test to HOLD." in captured.out

        refreshed = store.load("era5_land_daily_image_20200103_test")
        assert refreshed is not None
        assert refreshed.state == JobState.HOLD
        assert refreshed.attempt == 0
        assert refreshed.last_error is None
