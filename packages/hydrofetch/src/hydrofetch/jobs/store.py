"""JSON-file-based persistence store for :class:`~hydrofetch.jobs.models.JobRecord`.

One file per job: ``<job_dir>/<job_id>.json``.  Writing is atomic (write to a
temp file then rename) to avoid corrupt reads on crash.
"""

from __future__ import annotations

import logging
import os
import re
from collections import deque
from pathlib import Path
from typing import Iterator

from hydrofetch.jobs.models import (
    JobRecord,
    JobState,
    record_from_file,
    record_to_json,
)

log = logging.getLogger(__name__)

_STATE_RE = re.compile(r'"state"\s*:\s*"(\w+)"')


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
            return record_from_file(path)
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
    # Fast state scanning (without full JSON parse)
    # ------------------------------------------------------------------

    def _quick_state(self, path: Path) -> str | None:
        """Extract just the ``state`` value from a job file via regex.

        Much faster than full JSON parse for large files with embedded
        GeoJSON coordinates.
        """
        try:
            text = path.read_text(encoding="utf-8")
            m = _STATE_RE.search(text)
            return m.group(1) if m else None
        except Exception:  # pylint: disable=broad-except
            return None

    # ------------------------------------------------------------------
    # Recovery: scan once, classify, reset stalled → HOLD
    # ------------------------------------------------------------------

    def recover_stalled(self) -> tuple[deque[str], int]:
        """Scan all jobs once.  Reset in-flight (stalled) jobs to HOLD.

        Returns:
            ``(hold_queue, reset_count)`` where *hold_queue* is a
            :class:`~collections.deque` of job IDs in HOLD state (including
            those just reset), and *reset_count* is how many were reset.
        """
        hold_ids: deque[str] = deque()
        reset_count = 0

        for path in sorted(self._dir.glob("*.json")):
            state_str = self._quick_state(path)
            if state_str is None:
                continue

            job_id = path.stem

            if state_str == JobState.HOLD.value:
                hold_ids.append(job_id)
            elif state_str in (JobState.COMPLETED.value, JobState.FAILED.value):
                pass
            else:
                record = self.load(job_id)
                if record is None:
                    continue
                reset = record.advance(
                    JobState.HOLD,
                    task_id=None,
                    drive_file_id=None,
                )
                self.save(reset)
                hold_ids.append(job_id)
                reset_count += 1
                log.info(
                    "Recovery: reset stalled job %s (%s → HOLD)",
                    job_id,
                    state_str,
                )

        return hold_ids, reset_count

    # ------------------------------------------------------------------
    # Lazy iteration for hold jobs
    # ------------------------------------------------------------------

    def iter_hold_jobs(self) -> Iterator[JobRecord]:
        """Yield HOLD records one at a time without loading all files."""
        for path in sorted(self._dir.glob("*.json")):
            state_str = self._quick_state(path)
            if state_str != JobState.HOLD.value:
                continue
            try:
                yield record_from_file(path)
            except Exception as exc:  # pylint: disable=broad-except
                log.error("Skipping corrupt job file %s: %s", path, exc)

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
    # Counting
    # ------------------------------------------------------------------

    def count_active(self) -> int:
        """Return the number of non-terminal, non-HOLD records (holding a slot)."""
        count = 0
        for path in self._dir.glob("*.json"):
            state_str = self._quick_state(path)
            if state_str and state_str not in (
                JobState.HOLD.value,
                JobState.COMPLETED.value,
                JobState.FAILED.value,
            ):
                count += 1
        return count

    def summarise(self) -> dict[str, int]:
        """Return a count of records per :class:`~hydrofetch.jobs.models.JobState`."""
        counts: dict[str, int] = {s.value: 0 for s in JobState}
        for path in self._dir.glob("*.json"):
            state_str = self._quick_state(path)
            if state_str and state_str in counts:
                counts[state_str] += 1
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
