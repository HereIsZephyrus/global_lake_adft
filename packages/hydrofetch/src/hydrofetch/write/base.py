"""Abstract base class for output writers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from hydrofetch.jobs.models import JobRecord


class BaseWriter(ABC):
    """Write sampled forcing data from a completed job to a persistent sink."""

    @abstractmethod
    def write(self, record: JobRecord) -> None:
        """Persist the sample output referenced by *record*.

        The sample file path is available as ``record.local_sample_path``.

        Args:
            record: Completed or Write-state job record.

        Raises:
            ValueError: If the record has no sample path.
            IOError: On write failure.
        """


__all__ = ["BaseWriter"]
