"""Tests for state machine transitions using mocked external services."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from hydrofetch.jobs.models import (
    GeeExportParams,
    JobRecord,
    JobSpec,
    JobState,
    SampleParams,
    WriteParams,
)
from hydrofetch.monitor.throttle import ConcurrencyThrottle
from hydrofetch.state_machine.base import StateContext
from hydrofetch.state_machine.cleanup import CleanupState
from hydrofetch.state_machine.download import DownloadState
from hydrofetch.state_machine.export_state import ExportState
from hydrofetch.state_machine.hold import HoldState


def _make_record(state: JobState = JobState.HOLD, task_id: str | None = None) -> JobRecord:
    spec = JobSpec(
        job_id="sm_test_001",
        export_name="era5_land_daily_image_20200115",
        date_iso="2020-01-15",
        gee=GeeExportParams(
            spec_id="era5_land_daily_image",
            asset_id="ECMWF/ERA5_LAND/DAILY_AGGR",
            bands=["temperature_2m"],
            scale=11132.0,
            crs="EPSG:4326",
            max_pixels=10**13,
            region_geojson={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
            drive_folder="hf_exports",
        ),
        sample=SampleParams(geometry_path="/tmp/centroids.csv"),
        write=WriteParams(output_dir="/tmp/output"),
    )
    record = JobRecord(spec=spec, state=state, task_id=task_id)
    return record


def _make_context(tmp_path: Path, max_concurrent: int = 5, initial: int = 0) -> StateContext:
    drive = MagicMock()
    throttle = ConcurrencyThrottle(max_concurrent, initial_count=initial)
    return StateContext(
        drive=drive,
        throttle=throttle,
        raw_dir=tmp_path / "raw",
        sample_dir=tmp_path / "sample",
    )


# ---------------------------------------------------------------------------
# ConcurrencyThrottle unit tests
# ---------------------------------------------------------------------------


class TestConcurrencyThrottle:
    def test_acquire_and_release(self):
        t = ConcurrencyThrottle(max_concurrent=2)
        assert t.can_acquire()
        assert t.acquire()
        assert t.acquire()
        assert not t.can_acquire()
        t.release()
        assert t.can_acquire()

    def test_initial_count(self):
        t = ConcurrencyThrottle(max_concurrent=3, initial_count=2)
        assert t.current == 2
        assert t.can_acquire()
        assert t.acquire()
        assert not t.can_acquire()

    def test_release_does_not_go_below_zero(self):
        t = ConcurrencyThrottle(max_concurrent=2)
        t.release()
        assert t.current == 0

    def test_invalid_max(self):
        with pytest.raises(ValueError):
            ConcurrencyThrottle(max_concurrent=0)

    def test_invalid_initial(self):
        with pytest.raises(ValueError):
            ConcurrencyThrottle(max_concurrent=2, initial_count=-1)


# ---------------------------------------------------------------------------
# HoldState
# ---------------------------------------------------------------------------


class TestHoldState:
    def test_hold_waits_when_throttle_full(self, tmp_path):
        record = _make_record()
        context = _make_context(tmp_path, max_concurrent=1, initial=1)
        handler = HoldState()
        updated, next_state = handler.handle(record, context)
        assert updated is record
        assert next_state is None

    @patch("hydrofetch.state_machine.hold.submit_image_export", return_value="gee_task_xyz")
    def test_hold_submits_and_transitions_to_export(self, mock_submit, tmp_path):
        record = _make_record()
        context = _make_context(tmp_path, max_concurrent=2)
        handler = HoldState()
        updated, next_state = handler.handle(record, context)
        assert updated.state == JobState.EXPORT
        assert updated.task_id == "gee_task_xyz"
        assert isinstance(next_state, ExportState)
        assert context.throttle.current == 1

    @patch("hydrofetch.state_machine.hold.submit_image_export", side_effect=RuntimeError("GEE error"))
    def test_hold_releases_slot_on_failure(self, mock_submit, tmp_path):
        record = _make_record()
        context = _make_context(tmp_path, max_concurrent=2)
        handler = HoldState()
        updated, next_state = handler.handle(record, context)
        assert context.throttle.current == 0
        assert "GEE error" in updated.last_error


# ---------------------------------------------------------------------------
# ExportState
# ---------------------------------------------------------------------------


class TestExportState:
    @patch(
        "hydrofetch.state_machine.export_state.check_task_status",
        return_value={"state": "RUNNING"},
    )
    def test_export_waits_while_running(self, mock_status, tmp_path):
        record = _make_record(state=JobState.EXPORT, task_id="task_abc")
        context = _make_context(tmp_path)
        updated, next_state = ExportState().handle(record, context)
        assert updated is record
        assert next_state is None

    @patch(
        "hydrofetch.state_machine.export_state.check_task_status",
        return_value={"state": "COMPLETED"},
    )
    def test_export_transitions_to_download_on_completion(self, mock_status, tmp_path):
        record = _make_record(state=JobState.EXPORT, task_id="task_abc")
        context = _make_context(tmp_path)
        updated, next_state = ExportState().handle(record, context)
        assert updated.state == JobState.DOWNLOAD
        assert isinstance(next_state, DownloadState)

    @patch(
        "hydrofetch.state_machine.export_state.check_task_status",
        return_value={"state": "FAILED", "error_message": "out of memory"},
    )
    def test_export_fails_job_on_gee_failure(self, mock_status, tmp_path):
        record = _make_record(state=JobState.EXPORT, task_id="task_abc")
        context = _make_context(tmp_path, initial=1)
        updated, _ = ExportState().handle(record, context)
        assert "out of memory" in updated.last_error
        assert context.throttle.current == 0

    def test_export_requeues_when_no_task_id(self, tmp_path):
        record = _make_record(state=JobState.EXPORT, task_id=None)
        context = _make_context(tmp_path, initial=1)
        updated, next_state = ExportState().handle(record, context)
        assert updated.state == JobState.HOLD
        assert isinstance(next_state, HoldState)
        assert context.throttle.current == 0


# ---------------------------------------------------------------------------
# CleanupState
# ---------------------------------------------------------------------------


class TestCleanupState:
    def test_cleanup_deletes_drive_file_and_releases_slot(self, tmp_path):
        record = _make_record(state=JobState.CLEANUP)
        record = record.advance(
            JobState.CLEANUP,
            task_id="t1",
            drive_file_id="drive_abc",
            local_raw_path=str(tmp_path / "raw.tif"),
        )
        context = _make_context(tmp_path, initial=1)
        updated, next_state = CleanupState().handle(record, context)
        context.drive.delete_file.assert_called_once_with("drive_abc")
        assert context.throttle.current == 0
        assert updated.state == JobState.SAMPLE

    def test_cleanup_tolerates_failed_drive_delete(self, tmp_path):
        record = _make_record(state=JobState.CLEANUP)
        record = record.advance(JobState.CLEANUP, drive_file_id="drive_xyz")
        context = _make_context(tmp_path, initial=1)
        context.drive.delete_file.side_effect = Exception("permission denied")
        updated, next_state = CleanupState().handle(record, context)
        assert updated.state == JobState.SAMPLE
        assert context.throttle.current == 0
