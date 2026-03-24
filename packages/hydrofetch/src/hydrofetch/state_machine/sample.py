"""SampleState: read the downloaded raster and produce lake-level forcing values."""

from __future__ import annotations

import logging

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class SampleState(TaskState):
    """Compute area-weighted zonal means for each lake polygon from the downloaded GeoTIFF."""

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        from hydrofetch.sample.raster import (  # pylint: disable=import-outside-toplevel
            sample_raster_by_polygons_weighted,
        )
        from hydrofetch.state_machine.write_state import WriteState  # pylint: disable=import-outside-toplevel

        if not record.local_raw_path:
            log.error(
                "Job %s: local_raw_path is missing in Sample state", record.spec.job_id
            )
            return record.fail("local_raw_path missing in Sample state"), None

        import os  # pylint: disable=import-outside-toplevel
        from pathlib import Path  # pylint: disable=import-outside-toplevel

        raw_path = Path(record.local_raw_path)
        if not raw_path.is_file():
            log.error(
                "Job %s: raw file not found at %s", record.spec.job_id, raw_path
            )
            return record.fail(f"Raw file not found: {raw_path}"), None

        sample_params = record.spec.sample
        sample_out = context.sample_dir / f"{record.spec.export_name}_sampled.parquet"

        # Idempotency: if sampling output already exists, skip re-processing.
        if sample_out.is_file() and sample_out.stat().st_size > 0:
            log.info(
                "Job %s: sample output already exists at %s, skipping",
                record.spec.job_id,
                sample_out,
            )
            updated = record.advance(JobState.WRITE, local_sample_path=str(sample_out))
            return updated, WriteState()

        try:
            df = sample_raster_by_polygons_weighted(
                raster_path=raw_path,
                geometry_path=Path(sample_params.geometry_path),
                id_column=sample_params.id_column,
                date_iso=record.spec.date_iso,
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Job %s: zonal sampling failed: %s", record.spec.job_id, exc)
            return record.fail(f"Sampling error: {exc}"), None

        # Stage the output to sample_dir.
        try:
            sample_out.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(sample_out), index=False)
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Job %s: writing sample parquet failed: %s", record.spec.job_id, exc)
            return record.fail(f"Sample write error: {exc}"), None

        updated = record.advance(JobState.WRITE, local_sample_path=str(sample_out))
        log.info("Job %s: sampled %d rows to %s", record.spec.job_id, len(df), sample_out)
        return updated, WriteState()


__all__ = ["SampleState"]
