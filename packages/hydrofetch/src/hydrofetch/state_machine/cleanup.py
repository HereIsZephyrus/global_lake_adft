"""CleanupState: delete intermediate artefacts, release the concurrency slot,
and mark the job as COMPLETED.

This is the **final active state** in the pipeline::

    HOLD → EXPORT → DOWNLOAD → SAMPLE → WRITE → CLEANUP → COMPLETED

All cleanup operations are idempotent so a restart mid-cleanup simply
re-runs them without side-effects.
"""

from __future__ import annotations

import logging
import os

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class CleanupState(TaskState):
    """Delete Drive export, local intermediate files, and release the throttle slot."""

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        # 1. Delete the temporary Drive export file (non-fatal on failure).
        file_id = record.drive_file_id
        if file_id:
            try:
                context.drive.delete_file(file_id)
                log.info("Job %s: deleted Drive file %s", record.spec.job_id, file_id)
            except Exception as exc:  # pylint: disable=broad-except
                log.warning(
                    "Job %s: Drive delete failed (non-fatal): %s",
                    record.spec.job_id,
                    exc,
                )
        else:
            log.debug("Job %s: no drive_file_id to delete", record.spec.job_id)

        # 2. Delete local intermediate files.
        _delete_file(record.local_raw_path, record.spec.job_id, "raw GeoTIFF")
        if "file" not in record.spec.write.sinks:
            _delete_file(record.local_sample_path, record.spec.job_id, "sample Parquet")

        # 3. Release the concurrency slot.
        context.throttle.release()

        updated = record.advance(JobState.COMPLETED)
        log.info("Job %s: completed successfully", record.spec.job_id)
        return updated, None


def _delete_file(path: str | None, job_id: str, label: str) -> None:
    """Delete *path* if it exists; log a warning on failure (non-fatal)."""
    if not path:
        return
    try:
        os.unlink(path)
        log.debug("Job %s: deleted %s at %s", job_id, label, path)
    except FileNotFoundError:
        log.debug("Job %s: %s already absent at %s", job_id, label, path)
    except OSError as exc:
        log.warning(
            "Job %s: could not delete %s at %s: %s", job_id, label, path, exc
        )


__all__ = ["CleanupState"]
