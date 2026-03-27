"""WriteState: persist sampled outputs to the configured sink and mark job done.

File cleanup (raw GeoTIFF, staged Parquet) is **not** performed here.  It is
deferred to :func:`cleanup_after_write`, which the runner calls *after* the
COMPLETED state has been persisted to disk.  This eliminates the
"write-post-commit recovery gap" where a crash between file deletion and
state persistence would leave a job in WRITE state with its sample file
already gone.
"""

from __future__ import annotations

import logging
import os

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class WriteState(TaskState):
    """Write the staged sample output to the configured sink(s).

    On success the job transitions to COMPLETED.  Local intermediate files
    are **not** deleted here — the runner calls :func:`cleanup_after_write`
    after the state change has been safely persisted.
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


def cleanup_after_write(record: JobRecord) -> None:
    """Delete intermediate files for a COMPLETED job.

    Must be called **after** the COMPLETED record has been persisted to disk
    so that a crash during cleanup does not leave the job in an
    unrecoverable state.

    * ``local_raw_path`` (downloaded GeoTIFF) – always deleted.
    * ``local_sample_path`` (staged Parquet) – deleted only when the
      ``"file"`` sink is **not** in the job's write configuration.
    """
    _delete_file(record.local_raw_path, record.spec.job_id, "raw GeoTIFF")

    if "file" not in record.spec.write.sinks:
        _delete_file(record.local_sample_path, record.spec.job_id, "sample Parquet")


def _delete_file(path: str | None, job_id: str, label: str) -> None:
    """Delete *path* if it exists; log a warning on failure (non-fatal)."""
    if not path:
        return
    try:
        os.unlink(path)
        log.debug("Job %s: deleted %s at %s", job_id, label, path)
    except FileNotFoundError:
        log.debug("Job %s: %s already absent at %s", job_id, label, path)
    except OSError as exc:
        log.warning(
            "Job %s: could not delete %s at %s: %s", job_id, label, path, exc
        )


__all__ = ["WriteState", "cleanup_after_write"]
