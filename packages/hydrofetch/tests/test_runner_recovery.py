"""Tests for JobRunner recovery semantics and end-to-end state progression."""

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
from hydrofetch.jobs.store import JobStore
from hydrofetch.monitor.runner import JobRunner
from hydrofetch.monitor.throttle import ConcurrencyThrottle
from hydrofetch.state_machine.base import StateContext


def _make_spec(job_id: str, date_iso: str = "2020-03-10") -> JobSpec:
    return JobSpec(
        job_id=job_id,
        export_name=f"era5_land_daily_image_{date_iso.replace('-', '')}",
        date_iso=date_iso,
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


@pytest.fixture()
def runner_env(tmp_path):
    """Return a (store, context, runner) triple wired to tmp_path."""
    job_dir = tmp_path / "jobs"
    raw_dir = tmp_path / "raw"
    sample_dir = tmp_path / "sample"
    for d in (job_dir, raw_dir, sample_dir):
        d.mkdir()

    store = JobStore(job_dir)
    drive = MagicMock()
    throttle = ConcurrencyThrottle(max_concurrent=5, initial_count=0)
    context = StateContext(drive=drive, throttle=throttle, raw_dir=raw_dir, sample_dir=sample_dir)
    runner = JobRunner(store=store, context=context, poll_interval=0.01)
    return store, context, runner


class TestRunnerThrottleInit:
    """After a restart, active job count should seed the throttle."""

    def test_from_config_seeds_throttle(self, tmp_path):
        job_dir = tmp_path / "jobs"
        raw_dir = tmp_path / "raw"
        sample_dir = tmp_path / "sample"

        store = JobStore(job_dir)
        # Simulate 2 jobs already in-flight (EXPORT state = active)
        for i in range(2):
            rec = JobRecord(
                spec=_make_spec(f"job_{i}", f"2020-0{i+1}-01"),
                state=JobState.EXPORT,
                task_id=f"gee_task_{i}",
            )
            store.save(rec)

        drive = MagicMock()

        with patch("hydrofetch.monitor.runner.DriveClient"):
            runner = JobRunner.from_config(
                job_dir=job_dir,
                raw_dir=raw_dir,
                sample_dir=sample_dir,
                drive=drive,
                max_concurrent=5,
                poll_interval=0.01,
            )

        # 2 active jobs should be pre-counted in the throttle
        assert runner._context.throttle.current == 2  # pylint: disable=protected-access


class TestRunnerDeduplication:
    """Completed jobs should not be re-enqueued."""

    @patch("hydrofetch.state_machine.hold.submit_image_export", return_value="task_x")
    def test_completed_job_stays_completed(self, mock_submit, runner_env):
        store, context, runner = runner_env
        spec = _make_spec("job_done", "2020-04-01")
        rec = JobRecord(spec=spec, state=JobState.COMPLETED)
        store.save(rec)

        runner.step_once()

        loaded = store.load("job_done")
        assert loaded.state == JobState.COMPLETED
        mock_submit.assert_not_called()


class TestRunnerStepOnce:
    """step_once should advance HOLD jobs when slots are available."""

    @patch("hydrofetch.state_machine.hold.submit_image_export", return_value="task_new")
    def test_hold_advances_to_export(self, mock_submit, runner_env):
        store, context, runner = runner_env
        spec = _make_spec("job_hold", "2020-05-15")
        store.save(JobRecord(spec=spec))

        changed = runner.step_once()

        assert changed == 1
        loaded = store.load("job_hold")
        assert loaded.state == JobState.EXPORT
        assert loaded.task_id == "task_new"

    @patch(
        "hydrofetch.state_machine.export_state.check_task_status",
        return_value={"state": "RUNNING"},
    )
    def test_export_stays_when_running(self, mock_status, runner_env):
        store, context, runner = runner_env
        spec = _make_spec("job_export", "2020-06-01")
        rec = JobRecord(spec=spec, state=JobState.EXPORT, task_id="t1")
        store.save(rec)

        changed = runner.step_once()

        # No state change while task is RUNNING
        assert changed == 0
        loaded = store.load("job_export")
        assert loaded.state == JobState.EXPORT


class TestRunnerFailureAndRetry:
    """Failed jobs with remaining attempts should be re-queued to HOLD."""

    @patch(
        "hydrofetch.state_machine.export_state.check_task_status",
        return_value={"state": "FAILED", "error_message": "quota exceeded"},
    )
    def test_export_failure_requeues_to_hold_if_attempts_remain(self, mock_status, runner_env):
        store, context, runner = runner_env
        spec = _make_spec("job_fail_retry", "2020-07-01")
        rec = JobRecord(spec=spec, state=JobState.EXPORT, task_id="t_fail", max_attempts=3)
        context.throttle._count = 1  # pylint: disable=protected-access
        store.save(rec)

        runner.step_once()

        loaded = store.load("job_fail_retry")
        assert loaded.state == JobState.HOLD
        assert loaded.attempt == 1
        assert context.throttle.current == 0

    @patch(
        "hydrofetch.state_machine.export_state.check_task_status",
        return_value={"state": "FAILED", "error_message": "permanent error"},
    )
    def test_export_failure_becomes_failed_at_max_attempts(self, mock_status, runner_env):
        store, context, runner = runner_env
        spec = _make_spec("job_perm_fail", "2020-08-01")
        rec = JobRecord(spec=spec, state=JobState.EXPORT, task_id="t_perm", max_attempts=1)
        context.throttle._count = 1  # pylint: disable=protected-access
        store.save(rec)

        runner.step_once()

        loaded = store.load("job_perm_fail")
        assert loaded.state == JobState.FAILED
