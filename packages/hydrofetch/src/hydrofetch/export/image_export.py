"""Create and submit GEE ``Export.image.toDrive`` tasks for a single date."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

from hydrofetch.catalog.parser import ImageExportSpec

log = logging.getLogger(__name__)


def _next_day(d: date) -> date:
    return d + timedelta(days=1)


def build_era5_daily_image(
    spec: ImageExportSpec,
    day: date,
    region: dict[str, Any] | None = None,
) -> Any:
    """Return the single ``ee.Image`` that represents ERA5-Land for *day*.

    Filters the collection to a one-day window, takes the first image, and
    selects the requested bands.  When *region* is supplied the image is
    clipped to that geometry; otherwise the full ERA5-Land footprint is kept.

    Args:
        spec: Image export spec (supplies ``asset_id`` and ``band_names``).
        day: Calendar date.
        region: Optional GeoJSON-like dict (Feature or Geometry) defining the
            AOI.  ``None`` → no clip, full global footprint.

    Returns:
        An ``ee.Image`` ready for ``Export.image.toDrive``.
    """
    import ee  # pylint: disable=import-outside-toplevel,import-error

    start_str = f"{day.year:04d}-{day.month:02d}-{day.day:02d}"
    end_day = _next_day(day)
    end_str = f"{end_day.year:04d}-{end_day.month:02d}-{end_day.day:02d}"

    image = (
        ee.ImageCollection(spec.asset_id)
        .filterDate(start_str, end_str)
        .select(spec.band_names())
        .first()
    )

    if region is not None:
        geom_obj = ee.Geometry(region.get("geometry", region))
        image = image.clip(geom_obj)

    return image


def submit_image_export(
    spec: ImageExportSpec,
    day: date,
    region: dict[str, Any] | None,
    export_name: str,
    *,
    drive_folder: str | None = None,
) -> str:
    """Submit a GEE image export task and return the task ID.

    Args:
        spec: Dataset spec (band selection, scale, CRS).
        day: Calendar date for the single-day ERA5 image.
        region: Optional GeoJSON-like dict (Feature or Geometry) for the AOI.
            ``None`` exports the full ERA5-Land global footprint.
        export_name: Used as both the GEE task description and Drive fileNamePrefix.
        drive_folder: Target Google Drive folder name.  ``None`` → GEE default folder.

    Returns:
        The GEE ``task.id`` string that uniquely identifies this export job.

    Raises:
        RuntimeError: If the submitted task has no id (should not happen).
    """
    import ee  # pylint: disable=import-outside-toplevel,import-error

    image = build_era5_daily_image(spec, day, region)

    kwargs: dict[str, Any] = dict(
        image=image,
        description=export_name,
        fileNamePrefix=export_name,
        scale=spec.native_scale_m,
        crs=spec.crs,
        fileFormat=spec.file_format,
        maxPixels=spec.max_pixels,
    )
    if region is not None:
        kwargs["region"] = ee.Geometry(region.get("geometry", region))
    if drive_folder:
        kwargs["folder"] = drive_folder

    task = ee.batch.Export.image.toDrive(**kwargs)
    task.start()

    if not task.id:
        raise RuntimeError(f"Submitted GEE task has no id for export_name={export_name!r}")

    log.info(
        "GEE export submitted: task_id=%s export_name=%s day=%s region=%s",
        task.id,
        export_name,
        day,
        "global" if region is None else "clipped",
    )
    return task.id


__all__ = [
    "build_era5_daily_image",
    "submit_image_export",
]
