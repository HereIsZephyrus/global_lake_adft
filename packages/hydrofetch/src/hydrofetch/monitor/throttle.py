"""Concurrency throttle for GEE export slots.

Injected into :class:`~hydrofetch.monitor.runner.JobRunner` rather than
implemented as a global singleton so that tests can use independent instances
without teardown.
"""

from __future__ import annotations

import threading


class ConcurrencyThrottle:
    """Thread-safe counter that limits the number of concurrently active jobs.

    Args:
        max_concurrent: Maximum number of jobs that may hold a slot simultaneously.
        initial_count: Pre-seed the counter (used when recovering after a restart
            to account for jobs that already hold slots).
    """

    def __init__(self, max_concurrent: int, initial_count: int = 0) -> None:
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be >= 1")
        if initial_count < 0:
            raise ValueError("initial_count must be >= 0")
        self._max = max_concurrent
        self._count = initial_count
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def max_concurrent(self) -> int:
        """Maximum number of concurrent slots."""
        return self._max

    @property
    def current(self) -> int:
        """Current number of held slots."""
        with self._lock:
            return self._count

    # ------------------------------------------------------------------
    # Slot management
    # ------------------------------------------------------------------

    def can_acquire(self) -> bool:
        """Return True if a new slot is available."""
        with self._lock:
            return self._count < self._max

    def acquire(self) -> bool:
        """Acquire a slot if available.

        Returns:
            True if the slot was acquired, False if at capacity.
        """
        with self._lock:
            if self._count < self._max:
                self._count += 1
                return True
            return False

    def release(self) -> None:
        """Release one slot.  The counter will not go below zero."""
        with self._lock:
            self._count = max(0, self._count - 1)

    def __repr__(self) -> str:
        return f"ConcurrencyThrottle({self._count}/{self._max})"


__all__ = ["ConcurrencyThrottle"]
