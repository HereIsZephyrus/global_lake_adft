"""Tests for job record serialisation and deserialization."""

from __future__ import annotations

import json

import pytest

from hydrofetch.jobs.models import (
    GeeExportParams,
    JobRecord,
    JobSpec,
    JobState,
    SampleParams,
    WriteParams,
    record_from_dict,
    record_from_json,
    record_to_dict,
    record_to_json,
)


def _make_record(job_id: str = "test_job_001") -> JobRecord:
    spec = JobSpec(
        job_id=job_id,
        export_name=f"era5_{job_id}",
        date_iso="2020-01-15",
        gee=GeeExportParams(
            spec_id="era5_land_daily_image",
            asset_id="ECMWF/ERA5_LAND/DAILY_AGGR",
            bands=["temperature_2m", "total_precipitation_sum"],
            scale=11132.0,
            crs="EPSG:4326",
            max_pixels=10_000_000_000_000,
            region_geojson={"type": "Polygon", "coordinates": [[[70, 30], [90, 30], [90, 45], [70, 45], [70, 30]]]},
            drive_folder="hydrofetch_exports",
        ),
        sample=SampleParams(
            geometry_path="/data/lake_centroids.csv",
            id_column="hylak_id",
        ),
        write=WriteParams(
            output_dir="/data/output",
            output_format="parquet",
        ),
    )
    return JobRecord(spec=spec)


class TestJobStateMethods:
    def test_is_terminal_completed(self):
        assert JobState.COMPLETED.is_terminal

    def test_is_terminal_failed(self):
        assert JobState.FAILED.is_terminal

    def test_not_terminal_active_states(self):
        for s in (JobState.HOLD, JobState.EXPORT, JobState.DOWNLOAD, JobState.CLEANUP, JobState.SAMPLE, JobState.WRITE):
            assert not s.is_terminal

    def test_is_active_excludes_hold_and_terminal(self):
        assert not JobState.HOLD.is_active
        assert not JobState.COMPLETED.is_active
        assert not JobState.FAILED.is_active
        assert JobState.EXPORT.is_active
        assert JobState.SAMPLE.is_active


class TestJobRecordAdvance:
    def test_advance_changes_state(self):
        record = _make_record()
        updated = record.advance(JobState.EXPORT, task_id="abc123")
        assert updated.state == JobState.EXPORT
        assert updated.task_id == "abc123"

    def test_advance_immutable_original(self):
        record = _make_record()
        _ = record.advance(JobState.EXPORT, task_id="abc123")
        assert record.state == JobState.HOLD
        assert record.task_id is None

    def test_advance_updates_timestamp(self):
        record = _make_record()
        updated = record.advance(JobState.EXPORT)
        assert updated.updated_at >= record.updated_at

    def test_fail_increments_attempt(self):
        record = _make_record()
        failed = record.fail("network error")
        assert failed.attempt == 1
        assert failed.last_error == "network error"

    def test_fail_becomes_failed_at_max_attempts(self):
        record = _make_record()
        record = record.fail("err1")
        record = record.fail("err2")
        record = record.fail("err3")
        assert record.state == JobState.FAILED

    def test_fail_stays_in_current_state_below_max(self):
        record = _make_record().advance(JobState.EXPORT)
        failed = record.fail("transient error")
        assert failed.state == JobState.EXPORT  # stays in Export, attempt=1


class TestJobRecordSerialization:
    def test_roundtrip_dict(self):
        record = _make_record()
        d = record_to_dict(record)
        restored = record_from_dict(d)
        assert restored.spec.job_id == record.spec.job_id
        assert restored.state == record.state
        assert restored.spec.gee.bands == record.spec.gee.bands

    def test_roundtrip_json(self):
        record = _make_record()
        js = record_to_json(record)
        parsed = json.loads(js)
        assert parsed["spec"]["job_id"] == record.spec.job_id
        restored = record_from_json(js)
        assert restored.spec.date_iso == "2020-01-15"

    def test_advanced_record_roundtrip(self):
        record = _make_record().advance(
            JobState.DOWNLOAD,
            task_id="gee_task_xyz",
            drive_file_id="drive_abc",
        )
        restored = record_from_json(record_to_json(record))
        assert restored.state == JobState.DOWNLOAD
        assert restored.task_id == "gee_task_xyz"
        assert restored.drive_file_id == "drive_abc"

    def test_failed_record_roundtrip(self):
        record = _make_record().fail("something went wrong")
        restored = record_from_json(record_to_json(record))
        assert restored.last_error == "something went wrong"
        assert restored.attempt == 1
