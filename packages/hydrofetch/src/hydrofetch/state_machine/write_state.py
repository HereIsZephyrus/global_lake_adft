"""WriteState: persist sampled outputs to the configured sink and mark job done."""

from __future__ import annotations

import logging
import os

from hydrofetch.jobs.models import JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class WriteState(TaskState):
    """Write the staged sample output to the configured sink(s) and clean up.

    After a successful write the following local intermediate files are
    removed to keep disk usage minimal:

    * ``local_raw_path`` (downloaded GeoTIFF) – always deleted.
    * ``local_sample_path`` (staged Parquet) – deleted only when the
      ``"file"`` sink is **not** in the job's write configuration, because
      the ``FileWriter`` has already copied it to the final output directory
      so the staged copy is redundant; when ``"db"`` is the only sink the
      staged Parquet has no further value.
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

        # ------------------------------------------------------------------
        # Clean up local intermediate files.
        # ------------------------------------------------------------------
        self._delete_file(record.local_raw_path, record.spec.job_id, "raw GeoTIFF")

        # Remove staged sample Parquet only when the file sink is absent
        # (FileWriter already copied it to the output dir; no need for both).
        if "file" not in record.spec.write.sinks:
            self._delete_file(
                record.local_sample_path, record.spec.job_id, "sample Parquet"
            )

        updated = record.advance(JobState.COMPLETED)
        log.info("Job %s: completed successfully", record.spec.job_id)
        return updated, None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
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


__all__ = ["WriteState"]
