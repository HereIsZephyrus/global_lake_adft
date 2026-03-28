"""HoldState: fast-track recovery, deduplication, and concurrency-slot acquisition."""

from __future__ import annotations

import logging
import os
from datetime import date

from hydrofetch.export.image_export import submit_image_export
from hydrofetch.jobs.models import GeeExportParams, JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class HoldState(TaskState):
    """Wait for a concurrency slot, then start (or resume) the pipeline.

    After a restart-recovery (stalled jobs reset to HOLD), local artefacts
    from a previous run may already exist.  This handler fast-tracks through
    stages whose output is already on disk or on Drive:

    * ``local_sample_path`` exists on disk  → skip to WRITE
    * ``local_raw_path`` exists on disk     → skip to SAMPLE
    * ``drive_file_id`` set or tif on Drive → skip to DOWNLOAD
    * otherwise                             → submit a fresh GEE export
    """

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        # ---- fast-track: sample already on disk → skip to WRITE ----------
        if record.local_sample_path and os.path.isfile(record.local_sample_path):
            if not context.throttle.acquire():
                return record, None
            log.info(
                "Job %s: sample already exists at %s, fast-tracking to WRITE",
                record.spec.job_id,
                record.local_sample_path,
            )
            return record.advance(JobState.WRITE), None

        # ---- fast-track: raw raster already on disk → skip to SAMPLE -----
        if record.local_raw_path and os.path.isfile(record.local_raw_path):
            if not context.throttle.acquire():
                return record, None
            log.info(
                "Job %s: raw file already exists at %s, fast-tracking to SAMPLE",
                record.spec.job_id,
                record.local_raw_path,
            )
            return record.advance(JobState.SAMPLE), None

        # ---- fast-track: Drive file known or discoverable → DOWNLOAD -----
        file_id = record.drive_file_id
        if not file_id:
            file_id = self._probe_drive(record, context)
        if file_id:
            if not context.throttle.acquire():
                return record, None
            log.info(
                "Job %s: Drive file %s found, fast-tracking to DOWNLOAD",
                record.spec.job_id,
                file_id,
            )
            return record.advance(JobState.DOWNLOAD, drive_file_id=file_id), None

        # ---- normal path: acquire slot and submit GEE export -------------
        if not context.throttle.acquire():
            log.debug(
                "Job %s: no concurrency slot available (%s), staying in Hold",
                record.spec.job_id,
                context.throttle,
            )
            return record, None

        gee: GeeExportParams = record.spec.gee
        day = date.fromisoformat(record.spec.date_iso)

        try:
            task_id = submit_image_export(
                spec=_params_to_mock_spec(gee),
                day=day,
                region=gee.region_geojson,
                export_name=record.spec.export_name,
                drive_folder=gee.drive_folder,
            )
        except Exception as exc:  # pylint: disable=broad-except
            context.throttle.release()
            log.error("Job %s: GEE submit failed: %s", record.spec.job_id, exc)
            return record.fail(str(exc)), None

        updated = record.advance(JobState.EXPORT, task_id=task_id)
        log.info("Job %s: submitted GEE task %s", record.spec.job_id, task_id)
        return updated, None


    @staticmethod
    def _probe_drive(record: JobRecord, context: StateContext) -> str | None:
        """Check Google Drive for an already-exported tif matching *record*.

        Returns the Drive file ID if found, ``None`` otherwise.  Exceptions
        are swallowed so that a transient Drive error never blocks the queue.
        """
        try:
            files = context.drive.find_files_by_name_prefix(
                record.spec.export_name,
                folder_name=record.spec.gee.drive_folder,
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.debug(
                "Job %s: Drive probe failed (non-fatal): %s",
                record.spec.job_id,
                exc,
            )
            return None

        tif_files = [f for f in files if f["name"].endswith(".tif")]
        if tif_files:
            return tif_files[0]["id"]
        return None


def _params_to_mock_spec(gee: GeeExportParams):  # type: ignore[return]
    """Convert :class:`GeeExportParams` to a minimal duck-typed spec for ``submit_image_export``."""
    from hydrofetch.catalog.parser import BandSpec, ImageExportSpec  # pylint: disable=import-outside-toplevel

    return ImageExportSpec(
        spec_id=gee.spec_id,
        asset_id=gee.asset_id,
        native_scale_m=gee.scale,
        crs=gee.crs,
        file_format="GeoTIFF",
        bands=tuple(BandSpec(name=b) for b in gee.bands),
        temporal_granularity="preaggregated_daily",
        max_pixels=gee.max_pixels,
    )


__all__ = ["HoldState"]
