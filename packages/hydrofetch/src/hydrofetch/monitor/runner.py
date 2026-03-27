"""JobRunner: drives job records through the state machine in a polling loop."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.jobs.store import JobStore
from hydrofetch.monitor.throttle import ConcurrencyThrottle
from hydrofetch.state_machine.base import StateContext, TaskState
from hydrofetch.state_machine.cleanup import CleanupState
from hydrofetch.drive.client import DriveClient
from hydrofetch.state_machine.download import DownloadState
from hydrofetch.state_machine.export_state import ExportState
from hydrofetch.state_machine.hold import HoldState
from hydrofetch.state_machine.sample import SampleState
from hydrofetch.state_machine.write_state import WriteState, cleanup_after_write

log = logging.getLogger(__name__)

_STATE_HANDLERS: dict[JobState, type[TaskState]] = {
    JobState.HOLD: HoldState,
    JobState.EXPORT: ExportState,
    JobState.DOWNLOAD: DownloadState,
    JobState.CLEANUP: CleanupState,
    JobState.SAMPLE: SampleState,
    JobState.WRITE: WriteState,
}


class JobRunner:
    """Synchronous polling runner that advances jobs through the state machine.

    Args:
        store: Persistence store for job records.
        context: Shared services (Drive, throttle, directories).
        poll_interval: Seconds to sleep between full-cycle polls.
    """

    def __init__(
        self,
        store: JobStore,
        context: StateContext,
        poll_interval: float = 15.0,
    ) -> None:
        self._store = store
        self._context = context
        self._poll_interval = poll_interval

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def step_once(self) -> int:
        """Advance every active job by one state-machine step.

        Returns:
            Number of jobs that changed state in this cycle.
        """
        active = self._store.load_active()
        if not active:
            return 0

        changed = 0
        for record in active:
            new_record = self._step_job(record)
            if new_record is not record:
                self._store.save(new_record)
                changed += 1
                # Delete intermediate files only AFTER the COMPLETED state
                # has been safely persisted, preventing the recovery gap
                # where a crash could leave WRITE state with no sample file.
                if new_record.state == JobState.COMPLETED:
                    cleanup_after_write(new_record)
        return changed

    def run_until_done(self) -> None:
        """Block until all non-terminal jobs have completed or failed.

        Calls :meth:`step_once` in a tight loop, sleeping
        ``poll_interval`` seconds between cycles.
        """
        log.info(
            "JobRunner starting (poll_interval=%.0fs, throttle=%s)",
            self._poll_interval,
            self._context.throttle,
        )
        while True:
            active = self._store.load_active()
            if not active:
                log.info("No active jobs remain.  Runner finished.")
                break
            changed = self.step_once()
            summary = self._store.summarise()
            log.info("Poll cycle done: %d changed | %s", changed, summary)
            time.sleep(self._poll_interval)

    # ------------------------------------------------------------------
    # Single-job advancement
    # ------------------------------------------------------------------

    def _step_job(self, record: JobRecord) -> JobRecord:
        """Run the state handler for *record* and return the (possibly new) record."""
        handler_cls = _STATE_HANDLERS.get(record.state)
        if handler_cls is None:
            log.debug(
                "Job %s is in terminal state %s, nothing to do",
                record.spec.job_id,
                record.state.value,
            )
            return record

        handler = handler_cls()
        try:
            updated, _next_handler = handler.handle(record, self._context)
        except Exception as exc:  # pylint: disable=broad-except
            log.exception(
                "Job %s: unhandled exception in %s: %s",
                record.spec.job_id,
                handler_cls.__name__,
                exc,
            )
            updated = record.fail(f"Unhandled exception: {exc}")

        return updated

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        job_dir: Path,
        raw_dir: Path,
        sample_dir: Path,
        drive: "DriveClient",
        max_concurrent: int = 5,
        poll_interval: float = 15.0,
    ) -> "JobRunner":
        """Create a :class:`JobRunner` pre-seeded with the current active job count.

        Loading the active count before creating the throttle ensures that jobs
        already in-flight (e.g. from a previous run) are counted against the
        limit from the start.
        """
        store = JobStore(job_dir)
        active_count = store.count_active()

        throttle = ConcurrencyThrottle(
            max_concurrent=max_concurrent,
            initial_count=active_count,
        )
        log.info(
            "Throttle initialised: max=%d active=%d",
            max_concurrent,
            active_count,
        )

        context = StateContext(
            drive=drive,
            throttle=throttle,
            raw_dir=raw_dir,
            sample_dir=sample_dir,
        )
        return cls(store=store, context=context, poll_interval=poll_interval)


__all__ = ["JobRunner"]
