"""DownloadState: locate the Drive file and stream it to local raw storage."""

from __future__ import annotations

import logging
from pathlib import Path

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class DownloadState(TaskState):
    """Find the exported GeoTIFF on Google Drive and download it locally."""

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        from hydrofetch.state_machine.cleanup import (  # pylint: disable=import-outside-toplevel
            CleanupState,
        )

        dest = context.raw_dir / f"{record.spec.export_name}.tif"

        # Idempotency: if the file is already on disk (from a previous run that
        # crashed before saving the updated record), skip the download.
        if dest.is_file() and dest.stat().st_size > 0:
            log.info(
                "Job %s: raw file already exists at %s, skipping download",
                record.spec.job_id,
                dest,
            )
            updated = record.advance(
                JobState.CLEANUP,
                local_raw_path=str(dest),
            )
            return updated, CleanupState()

        # Resolve Drive file ID if not already stored.
        file_id = record.drive_file_id
        if not file_id:
            file_id = self._find_drive_file(record, context)
            if not file_id:
                # File not available yet – wait for the next cycle.
                return record, None

        try:
            context.drive.download_file(file_id, dest)
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Job %s: download failed: %s", record.spec.job_id, exc)
            context.throttle.release()
            failed = record.fail(f"Download error: {exc}")
            if not failed.state.is_terminal:
                failed = failed.advance(JobState.HOLD)
                from hydrofetch.state_machine.hold import HoldState  # pylint: disable=import-outside-toplevel

                return failed, HoldState()
            return failed, None

        updated = record.advance(
            JobState.CLEANUP,
            drive_file_id=file_id,
            local_raw_path=str(dest),
        )
        log.info("Job %s: downloaded to %s", record.spec.job_id, dest)
        return updated, CleanupState()

    @staticmethod
    def _find_drive_file(record: JobRecord, context: StateContext) -> str | None:
        """Return the Drive file ID for the export, or None if not found yet."""
        drive_folder = record.spec.gee.drive_folder
        try:
            files = context.drive.find_files_by_name_prefix(
                record.spec.export_name,
                folder_name=drive_folder,
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(
                "Job %s: Drive file search error: %s – will retry",
                record.spec.job_id,
                exc,
            )
            return None

        # GEE may produce one or more TIF shards; prefer the direct match.
        tif_files = [f for f in files if f["name"].endswith(".tif")]
        if not tif_files:
            log.debug(
                "Job %s: no .tif files found for prefix %r yet",
                record.spec.job_id,
                record.spec.export_name,
            )
            return None
        return tif_files[0]["id"]


__all__ = ["DownloadState"]
