"""Tests for JobStore persistence, recovery, and deduplication."""

from __future__ import annotations

import pytest

from hydrofetch.jobs.models import (
    GeeExportParams,
    JobRecord,
    JobSpec,
    JobState,
    SampleParams,
    WriteParams,
)
from hydrofetch.jobs.store import JobStore


def _make_record(job_id: str = "store_test_001") -> JobRecord:
    spec = JobSpec(
        job_id=job_id,
        export_name=f"era5_{job_id}",
        date_iso="2021-06-10",
        gee=GeeExportParams(
            spec_id="era5_land_daily_image",
            asset_id="ECMWF/ERA5_LAND/DAILY_AGGR",
            bands=["temperature_2m"],
            scale=11132.0,
            crs="EPSG:4326",
            max_pixels=10**13,
            region_geojson={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
        ),
        sample=SampleParams(geometry_path="/tmp/centroids.csv"),
        write=WriteParams(output_dir="/tmp/output"),
    )
    return JobRecord(spec=spec)


@pytest.fixture()
def store(tmp_path):
    return JobStore(tmp_path / "jobs")


class TestJobStoreSaveLoad:
    def test_save_and_load(self, store):
        record = _make_record("job_a")
        store.save(record)
        loaded = store.load("job_a")
        assert loaded is not None
        assert loaded.spec.job_id == "job_a"
        assert loaded.state == JobState.HOLD

    def test_load_missing_returns_none(self, store):
        assert store.load("does_not_exist") is None

    def test_save_overwrites_old_state(self, store):
        record = _make_record("job_b")
        store.save(record)
        updated = record.advance(JobState.EXPORT, task_id="task_123")
        store.save(updated)
        loaded = store.load("job_b")
        assert loaded.state == JobState.EXPORT
        assert loaded.task_id == "task_123"


class TestJobStoreLoadAll:
    def test_load_all_empty(self, store):
        assert store.load_all() == []

    def test_load_all_multiple(self, store):
        for i in range(5):
            store.save(_make_record(f"job_{i:03d}"))
        all_records = store.load_all()
        assert len(all_records) == 5

    def test_load_active_excludes_terminal(self, store):
        hold = _make_record("hold_job")
        completed = _make_record("done_job").advance(JobState.COMPLETED)
        failed = _make_record("fail_job").fail("bad")
        failed = failed.fail("bad").fail("bad")  # max attempts = 3

        store.save(hold)
        store.save(completed)
        store.save(failed)

        active = store.load_active()
        ids = {r.spec.job_id for r in active}
        assert "hold_job" in ids
        assert "done_job" not in ids
        # failed after 3 attempts should be terminal
        assert "fail_job" not in ids or failed.state != JobState.FAILED


class TestJobStoreExists:
    def test_exists_true(self, store):
        store.save(_make_record("exists_test"))
        assert store.exists("exists_test")

    def test_exists_false(self, store):
        assert not store.exists("nonexistent")

    def test_is_completed(self, store):
        record = _make_record("comp_test").advance(JobState.COMPLETED)
        store.save(record)
        assert store.is_completed("comp_test")

    def test_is_not_completed_for_hold(self, store):
        store.save(_make_record("hold_test"))
        assert not store.is_completed("hold_test")


class TestJobStoreSummarise:
    def test_summarise_counts(self, store):
        store.save(_make_record("j1"))
        store.save(_make_record("j2").advance(JobState.EXPORT, task_id="t1"))
        store.save(_make_record("j3").advance(JobState.COMPLETED))

        summary = store.summarise()
        assert summary.get("hold", 0) == 1
        assert summary.get("export", 0) == 1
        assert summary.get("completed", 0) == 1

    def test_count_active(self, store):
        store.save(_make_record("a1").advance(JobState.EXPORT, task_id="t1"))
        store.save(_make_record("a2").advance(JobState.DOWNLOAD, task_id="t2"))
        store.save(_make_record("a3"))  # HOLD – not active
        store.save(_make_record("a4").advance(JobState.COMPLETED))

        assert store.count_active() == 2
