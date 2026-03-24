"""Abstract base for state handlers and the shared context object."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from hydrofetch.drive.client import DriveClient
from hydrofetch.jobs.models import JobRecord
from hydrofetch.monitor.throttle import ConcurrencyThrottle


@dataclass
class StateContext:
    """Services and configuration injected into each state handler.

    Attributes:
        drive: Authenticated Google Drive client.
        throttle: Concurrency limiter shared by all jobs.
        raw_dir: Directory for downloaded raw GeoTIFF files.
        sample_dir: Directory for sampled lake-forcing outputs.
    """

    drive: DriveClient
    throttle: ConcurrencyThrottle
    raw_dir: Path
    sample_dir: Path


class TaskState(ABC):
    """One node in the job state machine.

    A :class:`TaskState` is a *stateless handler*: all mutable data lives in
    :class:`~hydrofetch.jobs.models.JobRecord`.  Implementations must be
    idempotent – the monitor may call :meth:`handle` multiple times for the
    same record (e.g. after a restart) until a state transition occurs.
    """

    @abstractmethod
    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, "TaskState | None"]:
        """Advance the job by one step.

        Args:
            record: Current immutable job record snapshot.
            context: Injected services (Drive, throttle, directories).

        Returns:
            A 2-tuple of the (possibly updated) :class:`JobRecord` and the
            **next** state handler, or ``None`` to remain in the current state
            until the next poll cycle (i.e. still waiting for something external).
        """


__all__ = ["StateContext", "TaskState"]
