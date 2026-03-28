"""WriteState: persist sampled outputs to the configured sink(s).

On success the job transitions to CLEANUP, which handles intermediate file
deletion and throttle slot release.
"""

from __future__ import annotations

import logging

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class WriteState(TaskState):
    """Write the staged sample output to the configured sink(s).

    On success the job transitions to CLEANUP.
    """

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        from hydrofetch.state_machine.cleanup import (  # pylint: disable=import-outside-toplevel
            CleanupState,
        )
        from hydrofetch.write.factory import get_writer  # pylint: disable=import-outside-toplevel

        if not record.local_sample_path:
            log.error("Job %s: local_sample_path missing in Write state", record.spec.job_id)
            return record.fail("local_sample_path missing in Write state"), None

        try:
            writer = get_writer(record.spec.write)
            writer.write(record)
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Job %s: write failed: %s", record.spec.job_id, exc)
            return record.fail(f"Write error: {exc}"), None

        updated = record.advance(JobState.CLEANUP)
        log.info("Job %s: write succeeded, advancing to cleanup", record.spec.job_id)
        return updated, CleanupState()


__all__ = ["WriteState"]
