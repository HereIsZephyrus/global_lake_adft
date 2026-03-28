"""Tests for the WRITE → CLEANUP → COMPLETED pipeline.

WriteState persists sampled outputs and transitions to CLEANUP.
CleanupState deletes Drive/local artefacts, releases the throttle slot,
and transitions to COMPLETED.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
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
from hydrofetch.state_machine.write_state import WriteState


def _make_context(tmp_path: Path) -> StateContext:
    return StateContext(
        drive=MagicMock(),
        throttle=ConcurrencyThrottle(max_concurrent=5),
        raw_dir=tmp_path / "raw",
        sample_dir=tmp_path / "sample",
    )


def _make_record(
    tmp_path: Path,
    sinks: list[str] | None = None,
    raw_exists: bool = True,
    sample_exists: bool = True,
    drive_file_id: str | None = "fake_drive_id",
) -> JobRecord:
    sinks = sinks or ["db"]
    raw_path = tmp_path / "raw" / "job.tif"
    sample_path = tmp_path / "sample" / "job_sampled.parquet"

    for d in (raw_path.parent, sample_path.parent):
        d.mkdir(parents=True, exist_ok=True)

    if raw_exists:
        raw_path.write_bytes(b"fake tif")
    if sample_exists:
        df = pd.DataFrame({"hylak_id": [1], "date": ["2020-01-01"], "temperature_2m": [280.0]})
        df.to_parquet(str(sample_path), index=False)

    spec = JobSpec(
        job_id="write_test",
        export_name="era5_land_daily_image_20200101_europe",
        date_iso="2020-01-01",
        gee=GeeExportParams(
            spec_id="era5_land_daily_image",
            asset_id="ECMWF/ERA5_LAND/DAILY_AGGR",
            bands=["temperature_2m"],
            scale=11132.0,
            crs="EPSG:4326",
            max_pixels=10**13,
            tile_id="europe",
        ),
        sample=SampleParams(
            geometry_path="/tmp/lakes.geojson",
            tile_id="europe",
        ),
        write=WriteParams(output_dir="", sinks=sinks),
    )
    return JobRecord(
        spec=spec,
        state=JobState.WRITE,
        drive_file_id=drive_file_id,
        local_raw_path=str(raw_path),
        local_sample_path=str(sample_path),
    )


class TestWriteState:
    """WriteState transitions to CLEANUP and does not delete files."""

    @patch("hydrofetch.write.factory.get_writer")
    def test_transitions_to_cleanup(self, mock_get_writer, tmp_path):
        mock_get_writer.return_value = MagicMock()
        record = _make_record(tmp_path)
        context = _make_context(tmp_path)

        updated, next_handler = WriteState().handle(record, context)

        assert updated.state == JobState.CLEANUP
        assert isinstance(next_handler, CleanupState)

    @patch("hydrofetch.write.factory.get_writer")
    def test_does_not_delete_files(self, mock_get_writer, tmp_path):
        mock_get_writer.return_value = MagicMock()
        record = _make_record(tmp_path)
        context = _make_context(tmp_path)

        WriteState().handle(record, context)

        assert Path(record.local_raw_path).exists()
        assert Path(record.local_sample_path).exists()


class TestCleanupState:
    """CleanupState deletes artefacts, releases throttle, transitions to COMPLETED."""

    def test_deletes_raw_and_sample_for_db_sink(self, tmp_path):
        record = _make_record(tmp_path, sinks=["db"])
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()

        raw_path = Path(record.local_raw_path)
        sample_path = Path(record.local_sample_path)

        updated, _ = CleanupState().handle(record, context)

        assert updated.state == JobState.COMPLETED
        assert not raw_path.exists()
        assert not sample_path.exists()
        assert context.throttle.current == 0

    def test_keeps_sample_for_file_sink(self, tmp_path):
        record = _make_record(tmp_path, sinks=["file"])
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()

        sample_path = Path(record.local_sample_path)
        updated, _ = CleanupState().handle(record, context)

        assert updated.state == JobState.COMPLETED
        assert sample_path.exists()

    def test_keeps_sample_for_file_and_db_sink(self, tmp_path):
        record = _make_record(tmp_path, sinks=["file", "db"])
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()

        sample_path = Path(record.local_sample_path)
        updated, _ = CleanupState().handle(record, context)

        assert updated.state == JobState.COMPLETED
        assert sample_path.exists()

    def test_releases_throttle(self, tmp_path):
        record = _make_record(tmp_path, sinks=["db"])
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()
        assert context.throttle.current == 1

        CleanupState().handle(record, context)

        assert context.throttle.current == 0

    def test_calls_drive_delete(self, tmp_path):
        record = _make_record(tmp_path, sinks=["db"], drive_file_id="abc123")
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()

        CleanupState().handle(record, context)

        context.drive.delete_file.assert_called_once_with("abc123")

    def test_drive_delete_failure_is_nonfatal(self, tmp_path):
        record = _make_record(tmp_path, sinks=["db"])
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()
        context.drive.delete_file.side_effect = RuntimeError("API error")

        updated, _ = CleanupState().handle(record, context)

        assert updated.state == JobState.COMPLETED

    def test_missing_raw_does_not_raise(self, tmp_path):
        record = _make_record(tmp_path, sinks=["db"], raw_exists=False)
        record = record.advance(JobState.CLEANUP)
        context = _make_context(tmp_path)
        context.throttle.acquire()

        updated, _ = CleanupState().handle(record, context)
        assert updated.state == JobState.COMPLETED
