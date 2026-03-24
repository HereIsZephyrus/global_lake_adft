"""HoldState: deduplication check and concurrency-slot acquisition."""

from __future__ import annotations

import logging
from datetime import date

from hydrofetch.export.image_export import submit_image_export
from hydrofetch.jobs.models import GeeExportParams, JobRecord, JobState
from hydrofetch.state_machine.base import StateContext, TaskState

log = logging.getLogger(__name__)


class HoldState(TaskState):
    """Wait for a concurrency slot, then submit the GEE export.

    Responsibilities:
    - Check whether the output already exists (idempotency).
    - Check whether a slot is available (throttle).
    - Submit the GEE Export.image.toDrive task.
    - Transition to :class:`~hydrofetch.state_machine.export_state.ExportState`.
    """

    def handle(
        self,
        record: JobRecord,
        context: StateContext,
    ) -> tuple[JobRecord, TaskState | None]:
        from hydrofetch.state_machine.export_state import (  # pylint: disable=import-outside-toplevel
            ExportState,
        )

        # Skip if the final output already exists (re-run idempotency).
        if record.local_sample_path:
            import os  # pylint: disable=import-outside-toplevel

            if os.path.isfile(record.local_sample_path):
                log.info(
                    "Job %s: sample output already exists at %s, marking completed",
                    record.spec.job_id,
                    record.local_sample_path,
                )
                updated = record.advance(JobState.COMPLETED)
                return updated, None

        if not context.throttle.acquire():
            log.debug(
                "Job %s: no concurrency slot available (%s), staying in Hold",
                record.spec.job_id,
                context.throttle,
            )
            return record, None

        # Slot acquired – submit the GEE task.
        gee: GeeExportParams = record.spec.gee
        day = date.fromisoformat(record.spec.date_iso)

        try:
            task_id = submit_image_export(
                spec=_params_to_mock_spec(gee),
                day=day,
                region=gee.region_geojson,
                export_name=record.spec.export_name,
                drive_folder=gee.drive_folder,
            )
        except Exception as exc:  # pylint: disable=broad-except
            context.throttle.release()
            log.error("Job %s: GEE submit failed: %s", record.spec.job_id, exc)
            return record.fail(str(exc)), None

        updated = record.advance(JobState.EXPORT, task_id=task_id)
        log.info("Job %s: submitted GEE task %s", record.spec.job_id, task_id)
        return updated, ExportState()


def _params_to_mock_spec(gee: GeeExportParams):  # type: ignore[return]
    """Convert :class:`GeeExportParams` to a minimal duck-typed spec for ``submit_image_export``."""
    from hydrofetch.catalog.parser import BandSpec, ImageExportSpec  # pylint: disable=import-outside-toplevel

    return ImageExportSpec(
        spec_id=gee.spec_id,
        asset_id=gee.asset_id,
        native_scale_m=gee.scale,
        crs=gee.crs,
        file_format="GeoTIFF",
        bands=tuple(BandSpec(name=b) for b in gee.bands),
        temporal_granularity="preaggregated_daily",
        max_pixels=gee.max_pixels,
    )


__all__ = ["HoldState"]
