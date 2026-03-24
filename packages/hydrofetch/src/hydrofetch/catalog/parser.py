"""Load and validate GEE image-export dataset specifications from JSON catalog files."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_VALID_FILE_FORMATS = frozenset({"GeoTIFF", "TFRecord"})
_VALID_TEMPORAL = frozenset({"preaggregated_daily", "single_image"})


@dataclass(frozen=True)
class BandSpec:
    """One band in a GEE image export specification."""

    name: str
    description: str = ""


@dataclass(frozen=True)
class ImageExportSpec:
    """Immutable specification for a GEE ``Export.image.toDrive`` workflow."""

    spec_id: str
    asset_id: str
    native_scale_m: float
    crs: str
    file_format: str
    bands: tuple[BandSpec, ...]
    temporal_granularity: str
    max_pixels: int
    notes: str = ""

    def band_names(self) -> list[str]:
        """Return band names in catalog order."""
        return [b.name for b in self.bands]


def _expect(cond: bool, msg: str) -> None:
    if not cond:
        raise ValueError(msg)


def _parse_bands(raw: Any) -> tuple[BandSpec, ...]:
    _expect(isinstance(raw, list) and len(raw) > 0, "bands must be a non-empty list")
    out: list[BandSpec] = []
    for i, item in enumerate(raw):
        _expect(isinstance(item, dict), f"bands[{i}] must be an object")
        name = item.get("name")
        _expect(
            isinstance(name, str) and name.strip(),
            f"bands[{i}].name must be a non-empty string",
        )
        description = item.get("description", "")
        out.append(BandSpec(name=name.strip(), description=str(description)))
    return tuple(out)


def load_image_spec(path: str | Path) -> ImageExportSpec:
    """Parse a catalog JSON file into an :class:`ImageExportSpec`.

    Args:
        path: Path to a ``*.json`` catalog file.

    Returns:
        Frozen :class:`ImageExportSpec`.

    Raises:
        ValueError: On invalid structure or unknown enum values.
        FileNotFoundError: If *path* does not exist.
    """
    p = Path(path)
    if not p.is_file():
        raise FileNotFoundError(f"Catalog file not found: {p}")
    with p.open(encoding="utf-8") as fh:
        data = json.load(fh)
    _expect(isinstance(data, dict), "root must be a JSON object")

    spec_id = data.get("id", "")
    _expect(isinstance(spec_id, str) and spec_id.strip(), "id must be a non-empty string")

    asset_id = data.get("asset_id", "")
    _expect(
        isinstance(asset_id, str) and asset_id.strip(),
        "asset_id must be a non-empty string",
    )

    native = data.get("native_scale_m")
    _expect(
        isinstance(native, (int, float)) and float(native) > 0,
        "native_scale_m must be a positive number",
    )

    crs = data.get("crs", "EPSG:4326")
    _expect(isinstance(crs, str) and crs.strip(), "crs must be a non-empty string")

    file_format = data.get("file_format", "GeoTIFF")
    _expect(
        isinstance(file_format, str) and file_format in _VALID_FILE_FORMATS,
        f"file_format must be one of {sorted(_VALID_FILE_FORMATS)}",
    )

    temporal = data.get("temporal_granularity", "preaggregated_daily")
    _expect(
        isinstance(temporal, str) and temporal in _VALID_TEMPORAL,
        f"temporal_granularity must be one of {sorted(_VALID_TEMPORAL)}",
    )

    max_px = data.get("max_pixels", 10**13)
    _expect(isinstance(max_px, int) and max_px >= 1, "max_pixels must be a positive integer")

    notes = str(data.get("notes", ""))

    bands = _parse_bands(data.get("bands", []))

    spec = ImageExportSpec(
        spec_id=spec_id.strip(),
        asset_id=asset_id.strip(),
        native_scale_m=float(native),
        crs=crs.strip(),
        file_format=file_format,
        bands=bands,
        temporal_granularity=temporal,
        max_pixels=int(max_px),
        notes=notes,
    )
    log.debug("Loaded image spec %s from %s", spec.spec_id, p)
    return spec


def bundled_spec_path(name: str = "era5_land_daily_image.json") -> Path:
    """Return the path to a bundled catalog file in this package directory."""
    return Path(__file__).resolve().parent / name


__all__ = [
    "BandSpec",
    "ImageExportSpec",
    "bundled_spec_path",
    "load_image_spec",
]
