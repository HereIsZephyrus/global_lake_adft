"""WriteState: persist sampled outputs to the configured sink and mark job done."""

from __future__ import annotations

import logging

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class WriteState(TaskState):
    """Write the staged sample output to the configured sink.

    For the ``file`` sink (v1), the sample Parquet is already written by
    :class:`~hydrofetch.state_machine.sample.SampleState`; this state just
    verifies the file and copies/moves it to the final output directory.
    """

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
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

        updated = record.advance(JobState.COMPLETED)
        log.info("Job %s: completed successfully", record.spec.job_id)
        return updated, None


__all__ = ["WriteState"]
