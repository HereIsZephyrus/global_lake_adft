"""Tests for WriteState and cleanup_after_write file cleanup.

WriteState itself no longer deletes files — it only transitions the job to
COMPLETED.  The runner calls ``cleanup_after_write`` *after* persisting the
COMPLETED state, so a crash between write and persist cannot lose the sample
file while the job is still in WRITE state.
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
from hydrofetch.state_machine.write_state import WriteState, cleanup_after_write


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
    record = JobRecord(
        spec=spec,
        state=JobState.WRITE,
        local_raw_path=str(raw_path),
        local_sample_path=str(sample_path),
    )
    return record


class TestWriteStateCleanup:
    """WriteState must NOT delete files — only transition to COMPLETED."""

    @patch("hydrofetch.write.factory.get_writer")
    def test_write_state_does_not_delete_files(self, mock_get_writer, tmp_path):
        """After WriteState.handle, files must still exist on disk."""
        mock_writer = MagicMock()
        mock_get_writer.return_value = mock_writer

        record = _make_record(tmp_path, sinks=["db"])
        raw_path = Path(record.local_raw_path)
        sample_path = Path(record.local_sample_path)
        context = _make_context(tmp_path)

        updated, _ = WriteState().handle(record, context)

        assert updated.state == JobState.COMPLETED
        assert raw_path.exists(), "WriteState must NOT delete raw file (runner does it after save)"
        assert sample_path.exists(), "WriteState must NOT delete sample file"

    @patch("hydrofetch.write.factory.get_writer")
    def test_cleanup_after_write_deletes_raw_for_db_sink(self, mock_get_writer, tmp_path):
        mock_writer = MagicMock()
        mock_get_writer.return_value = mock_writer

        record = _make_record(tmp_path, sinks=["db"])
        raw_path = Path(record.local_raw_path)
        context = _make_context(tmp_path)

        updated, _ = WriteState().handle(record, context)
        cleanup_after_write(updated)

        assert not raw_path.exists(), "raw GeoTIFF should be deleted by cleanup_after_write"

    @patch("hydrofetch.write.factory.get_writer")
    def test_cleanup_after_write_deletes_sample_for_db_only_sink(self, mock_get_writer, tmp_path):
        mock_writer = MagicMock()
        mock_get_writer.return_value = mock_writer

        record = _make_record(tmp_path, sinks=["db"])
        sample_path = Path(record.local_sample_path)
        context = _make_context(tmp_path)

        updated, _ = WriteState().handle(record, context)
        cleanup_after_write(updated)

        assert not sample_path.exists(), "staged sample Parquet should be deleted for db-only sink"

    @patch("hydrofetch.write.factory.get_writer")
    def test_cleanup_keeps_sample_for_file_sink(self, mock_get_writer, tmp_path):
        mock_writer = MagicMock()
        mock_get_writer.return_value = mock_writer

        record = _make_record(tmp_path, sinks=["file"])
        sample_path = Path(record.local_sample_path)
        context = _make_context(tmp_path)

        updated, _ = WriteState().handle(record, context)
        cleanup_after_write(updated)

        assert sample_path.exists(), (
            "staged sample Parquet should be kept when 'file' sink is present"
        )

    @patch("hydrofetch.write.factory.get_writer")
    def test_cleanup_keeps_sample_for_file_and_db_sink(self, mock_get_writer, tmp_path):
        mock_writer = MagicMock()
        mock_get_writer.return_value = mock_writer

        record = _make_record(tmp_path, sinks=["file", "db"])
        sample_path = Path(record.local_sample_path)
        context = _make_context(tmp_path)

        updated, _ = WriteState().handle(record, context)
        cleanup_after_write(updated)

        assert sample_path.exists()

    @patch("hydrofetch.write.factory.get_writer")
    def test_missing_raw_file_does_not_raise(self, mock_get_writer, tmp_path):
        """Non-existent raw path should be logged as a warning, not crash."""
        mock_writer = MagicMock()
        mock_get_writer.return_value = mock_writer

        record = _make_record(tmp_path, sinks=["db"], raw_exists=False)
        context = _make_context(tmp_path)

        updated, _ = WriteState().handle(record, context)
        assert updated.state == JobState.COMPLETED
        cleanup_after_write(updated)  # should not raise
