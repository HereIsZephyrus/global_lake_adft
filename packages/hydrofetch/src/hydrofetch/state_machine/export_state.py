"""ExportState: poll the GEE task until COMPLETED or FAILED."""

from __future__ import annotations

import logging

from hydrofetch.gee.client import check_task_status
from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)

# GEE task state strings returned by the EE Python API.
_GEE_DONE = frozenset({"COMPLETED"})
_GEE_FAILED = frozenset({"FAILED", "CANCELLED", "CANCEL_REQUESTED"})


class ExportState(TaskState):
    """Poll the GEE export task until it completes or fails."""

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        from hydrofetch.state_machine.download import (  # pylint: disable=import-outside-toplevel
            DownloadState,
        )

        if not record.task_id:
            # task_id missing after restart – re-enqueue from Hold.
            log.warning(
                "Job %s: no task_id in Export state; re-queuing to Hold",
                record.spec.job_id,
            )
            context.throttle.release()
            updated = record.advance(JobState.HOLD, task_id=None)
            from hydrofetch.state_machine.hold import HoldState  # pylint: disable=import-outside-toplevel

            return updated, HoldState()

        try:
            status = check_task_status(record.task_id)
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(
                "Job %s: could not check GEE status: %s – will retry next cycle",
                record.spec.job_id,
                exc,
            )
            return record, None  # stay in Export and try again next poll

        gee_state: str = status.get("state", "UNKNOWN")
        log.debug(
            "Job %s: GEE task %s state=%s",
            record.spec.job_id,
            record.task_id,
            gee_state,
        )

        if gee_state in _GEE_DONE:
            updated = record.advance(JobState.DOWNLOAD)
            return updated, DownloadState()

        if gee_state in _GEE_FAILED:
            error_msg = status.get("error_message", gee_state)
            log.error("Job %s: GEE task failed: %s", record.spec.job_id, error_msg)
            context.throttle.release()
            failed = record.fail(f"GEE task {gee_state}: {error_msg}")
            # If retries remain, re-queue from Hold.
            if not failed.state.is_terminal:
                failed = failed.advance(JobState.HOLD)
                from hydrofetch.state_machine.hold import HoldState  # pylint: disable=import-outside-toplevel

                return failed, HoldState()
            return failed, None

        # RUNNING / READY / UNSUBMITTED: wait for the next poll cycle.
        return record, None


__all__ = ["ExportState"]
