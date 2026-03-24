"""CleanupState: delete the Drive artefact and release the concurrency slot."""

from __future__ import annotations

import logging

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class CleanupState(TaskState):
    """Delete the temporary Drive export file and release the concurrency slot."""

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        from hydrofetch.state_machine.sample import (  # pylint: disable=import-outside-toplevel
            SampleState,
        )

        file_id = record.drive_file_id
        if file_id:
            try:
                context.drive.delete_file(file_id)
                log.info("Job %s: deleted Drive file %s", record.spec.job_id, file_id)
            except Exception as exc:  # pylint: disable=broad-except
                # A failed delete is non-fatal: the job can continue; the
                # orphaned Drive file can be cleaned up manually later.
                log.warning(
                    "Job %s: Drive delete failed (non-fatal): %s",
                    record.spec.job_id,
                    exc,
                )
        else:
            log.debug("Job %s: no drive_file_id to delete, skipping cleanup", record.spec.job_id)

        # Release the concurrency slot so another job can enter Export.
        context.throttle.release()

        updated = record.advance(JobState.SAMPLE)
        return updated, SampleState()


__all__ = ["CleanupState"]
