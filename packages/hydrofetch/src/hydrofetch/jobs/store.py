"""JSON-file-based persistence store for :class:`~hydrofetch.jobs.models.JobRecord`.

One file per job: ``<job_dir>/<job_id>.json``.  Writing is atomic (write to a
temp file then rename) to avoid corrupt reads on crash.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from hydrofetch.jobs.models import (
    JobRecord,
    JobState,
    record_from_file,
    record_to_dict,
    record_to_json,
)

log = logging.getLogger(__name__)


class JobStore:
    """Persist and load :class:`JobRecord` objects from a directory.

    Args:
        job_dir: Directory where ``<job_id>.json`` files are stored.
    """

    def __init__(self, job_dir: Path) -> None:
        self._dir = job_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------

    def _path(self, job_id: str) -> Path:
        return self._dir / f"{job_id}.json"

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save(self, record: JobRecord) -> None:
        """Atomically write *record* to ``<job_dir>/<job_id>.json``.

        Raises:
            ValueError: If ``record.spec.job_id`` is empty.
        """
        if not record.spec.job_id:
            raise ValueError("job_id must be non-empty")

        target = self._path(record.spec.job_id)
        tmp = target.with_suffix(".tmp")
        try:
            tmp.write_text(record_to_json(record), encoding="utf-8")
            os.replace(tmp, target)
        except Exception:
            tmp.unlink(missing_ok=True)
            raise
        log.debug("Saved job %s (state=%s)", record.spec.job_id, record.state.value)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load(self, job_id: str) -> JobRecord | None:
        """Load a single record by *job_id*.  Returns ``None`` if not found."""
        path = self._path(job_id)
        if not path.is_file():
            return None
        try:
            record = record_from_file(path)
            return record
        except Exception as exc:  # pylint: disable=broad-except
            log.error("Failed to load job %s: %s", job_id, exc)
            return None

    def load_all(self) -> list[JobRecord]:
        """Load every ``*.json`` file in the job directory.

        Corrupt files are logged and skipped.
        """
        records: list[JobRecord] = []
        for path in sorted(self._dir.glob("*.json")):
            try:
                records.append(record_from_file(path))
            except Exception as exc:  # pylint: disable=broad-except
                log.error("Skipping corrupt job file %s: %s", path, exc)
        return records

    def load_active(self) -> list[JobRecord]:
        """Return all records that are not in a terminal state."""
        return [r for r in self.load_all() if not r.state.is_terminal]

    # ------------------------------------------------------------------
    # Existence / deduplication
    # ------------------------------------------------------------------

    def exists(self, job_id: str) -> bool:
        """Return True if a record for *job_id* already exists on disk."""
        return self._path(job_id).is_file()

    def is_completed(self, job_id: str) -> bool:
        """Return True if *job_id* exists and is in COMPLETED state."""
        record = self.load(job_id)
        return record is not None and record.state == JobState.COMPLETED

    # ------------------------------------------------------------------
    # Counting (for concurrency initialisation after restart)
    # ------------------------------------------------------------------

    def count_active(self) -> int:
        """Return the number of non-terminal, non-HOLD records (holding a slot)."""
        return sum(1 for r in self.load_all() if r.state.is_active)

    def summarise(self) -> dict[str, int]:
        """Return a count of records per :class:`~hydrofetch.jobs.models.JobState`."""
        counts: dict[str, int] = {s.value: 0 for s in JobState}
        for record in self.load_all():
            counts[record.state.value] += 1
        return {k: v for k, v in counts.items() if v > 0}

    # ------------------------------------------------------------------
    # Representation helpers (for ``hydrofetch status``)
    # ------------------------------------------------------------------

    def print_summary(self) -> None:
        """Print a human-readable summary of all jobs to stdout."""
        summary = self.summarise()
        if not summary:
            print("No jobs found in", self._dir)
            return
        print(f"Job directory: {self._dir}")
        for state, count in sorted(summary.items()):
            print(f"  {state:<12} {count}")

    def print_all(self) -> None:
        """Print job_id and state for every record."""
        records = self.load_all()
        if not records:
            print("No jobs found in", self._dir)
            return
        for rec in sorted(records, key=lambda r: r.spec.job_id):
            err = f"  ({rec.last_error})" if rec.last_error else ""
            print(f"{rec.spec.job_id:<50} {rec.state.value}{err}")


__all__ = ["JobStore"]
