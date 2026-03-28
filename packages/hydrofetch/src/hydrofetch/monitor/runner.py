"""JobRunner: drives job records through the state machine in a polling loop.

On startup, stalled in-flight jobs (from a previous crashed run) are reset
to HOLD so they re-enter the pipeline via the fast-track checks in
:class:`~hydrofetch.state_machine.hold.HoldState`.

Only active (in-flight) job files are loaded each cycle; HOLD jobs are
pulled lazily from a queue built during the one-time recovery scan.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from pathlib import Path

from hydrofetch.drive.client import DriveClient
from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.jobs.store import JobStore
from hydrofetch.monitor.throttle import ConcurrencyThrottle
from hydrofetch.state_machine.base import StateContext, TaskState
from hydrofetch.state_machine.cleanup import CleanupState
from hydrofetch.state_machine.download import DownloadState
from hydrofetch.state_machine.export_state import ExportState
from hydrofetch.state_machine.hold import HoldState
from hydrofetch.state_machine.sample import SampleState
from hydrofetch.state_machine.write_state import WriteState

log = logging.getLogger(__name__)

_STATE_HANDLERS: dict[JobState, type[TaskState]] = {
    JobState.HOLD: HoldState,
    JobState.EXPORT: ExportState,
    JobState.DOWNLOAD: DownloadState,
    JobState.SAMPLE: SampleState,
    JobState.WRITE: WriteState,
    JobState.CLEANUP: CleanupState,
}


class JobRunner:
    """Synchronous polling runner that advances jobs through the state machine.

    Args:
        store: Persistence store for job records.
        context: Shared services (Drive, throttle, directories).
        poll_interval: Seconds to sleep between full-cycle polls.
        hold_queue: Pre-built queue of HOLD job IDs (from recovery scan).
    """

    def __init__(
        self,
        store: JobStore,
        context: StateContext,
        poll_interval: float = 15.0,
        hold_queue: deque[str] | None = None,
    ) -> None:
        self._store = store
        self._context = context
        self._poll_interval = poll_interval
        self._active_ids: set[str] = set()
        self._hold_queue: deque[str] = hold_queue or deque()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def step_once(self) -> int:
        """Advance every actionable job by one state-machine step.

        1. Process known active (in-flight) jobs by loading their files
           individually — O(active) not O(total).
        2. Pull HOLD jobs from the queue while the throttle has spare
           capacity.

        Returns:
            Number of jobs that changed state in this cycle.
        """
        changed = 0

        # --- 1. Process active (in-flight) jobs -------------------------
        # Jobs that fall back to HOLD during this phase are deferred to the
        # *next* cycle to avoid double-processing in a single step.
        deferred: list[str] = []

        for job_id in list(self._active_ids):
            record = self._store.load(job_id)
            if record is None:
                self._active_ids.discard(job_id)
                continue
            if record.state.is_terminal:
                self._active_ids.discard(job_id)
                continue
            if record.state == JobState.HOLD:
                self._active_ids.discard(job_id)
                deferred.append(job_id)
                continue

            new_record = self._step_job(record)
            if new_record is not record:
                self._store.save(new_record)
                changed += 1
                if new_record.state.is_terminal:
                    self._active_ids.discard(job_id)
                elif new_record.state == JobState.HOLD:
                    self._active_ids.discard(job_id)
                    deferred.append(job_id)

        # --- 2. Pull HOLD jobs while throttle has spare capacity --------
        # Process only jobs that were already queued; deferred items are
        # appended afterwards so they wait until the next cycle.
        hold_budget = len(self._hold_queue)
        attempts = 0
        while attempts < hold_budget and self._context.throttle.can_acquire():
            job_id = self._hold_queue.popleft()
            attempts += 1
            record = self._store.load(job_id)
            if record is None:
                continue
            if record.state != JobState.HOLD:
                if record.state.is_active:
                    self._active_ids.add(job_id)
                continue

            new_record = self._step_job(record)
            if new_record is record:
                self._hold_queue.appendleft(job_id)
                break
            self._store.save(new_record)
            changed += 1
            if new_record.state.is_active:
                self._active_ids.add(new_record.spec.job_id)

        # Deferred jobs re-enter the queue for the next cycle.
        self._hold_queue.extend(deferred)

        return changed

    def run_until_done(self) -> None:
        """Block until all non-terminal jobs have completed or failed.

        Calls :meth:`step_once` in a tight loop, sleeping
        ``poll_interval`` seconds between cycles.
        """
        log.info(
            "JobRunner starting (poll_interval=%.0fs, throttle=%s, "
            "hold_queue=%d)",
            self._poll_interval,
            self._context.throttle,
            len(self._hold_queue),
        )
        while True:
            changed = self.step_once()
            summary = self._store.summarise()
            non_terminal = sum(
                count for state, count in summary.items()
                if state not in ("completed", "failed")
            )
            log.info(
                "Poll cycle done: %d changed, active=%d, hold_queue=%d | %s",
                changed,
                len(self._active_ids),
                len(self._hold_queue),
                summary,
            )
            if non_terminal == 0:
                log.info("No active jobs remain.  Runner finished.")
                break
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
        """Create a :class:`JobRunner` with startup recovery.

        1. Scan all job files once (fast regex for state, full parse only
           for stalled jobs that need resetting).
        2. Reset stalled in-flight jobs to HOLD.
        3. Initialise the throttle at zero (all slots free after recovery).
        4. Return a runner with a pre-built HOLD queue ready to go.
        """
        store = JobStore(job_dir)

        hold_queue, reset_count = store.recover_stalled()
        if reset_count:
            log.info("Recovery: reset %d stalled job(s) to HOLD", reset_count)
        log.info(
            "Recovery complete: %d HOLD job(s) queued for processing",
            len(hold_queue),
        )

        throttle = ConcurrencyThrottle(max_concurrent=max_concurrent)
        context = StateContext(
            drive=drive,
            throttle=throttle,
            raw_dir=raw_dir,
            sample_dir=sample_dir,
        )
        return cls(
            store=store,
            context=context,
            poll_interval=poll_interval,
            hold_queue=hold_queue,
        )


__all__ = ["JobRunner"]
