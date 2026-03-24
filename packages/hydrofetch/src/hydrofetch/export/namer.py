"""Derive stable file-name prefixes for GEE ``Export.image.toDrive`` jobs."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, datetime


def _parse_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.strip()).date()
    raise TypeError(f"start/end must be date, datetime, or ISO date string; got {type(value)}")


def image_day_prefix(
    spec_id: str,
    day: date | datetime | str,
) -> str:
    """Return ``<spec_id>_<YYYYMMDD>`` for a single-day image export.

    Example: ``era5_land_daily_image_20200115``

    Args:
        spec_id: Catalog spec identifier (safe string, no path separators).
        day: The calendar date of the image.

    Returns:
        A string safe for use as ``fileNamePrefix``.
    """
    d = _parse_date(day)
    return f"{spec_id}_{d.year:04d}{d.month:02d}{d.day:02d}"


def image_range_prefix(
    spec_id: str,
    start: date | datetime | str,
    end_exclusive: date | datetime | str,
) -> str:
    """Return ``<spec_id>_<startYMD>_<endYMD>`` for a multi-day or composite export.

    Args:
        spec_id: Catalog spec identifier.
        start: Inclusive start date.
        end_exclusive: Exclusive end date.

    Returns:
        A string safe for use as ``fileNamePrefix``.
    """
    s = _parse_date(start)
    e = _parse_date(end_exclusive)
    return (
        f"{spec_id}"
        f"_{s.year:04d}{s.month:02d}{s.day:02d}"
        f"_{e.year:04d}{e.month:02d}{e.day:02d}"
    )


def iter_daily_date_range(
    start: date | datetime | str,
    end_exclusive: date | datetime | str,
) -> Iterator[date]:
    """Yield every calendar date in ``[start, end_exclusive)``."""
    from datetime import timedelta  # pylint: disable=import-outside-toplevel

    s = _parse_date(start)
    e = _parse_date(end_exclusive)
    current = s
    while current < e:
        yield current
        current += timedelta(days=1)


__all__ = [
    "image_day_prefix",
    "image_range_prefix",
    "iter_daily_date_range",
]
