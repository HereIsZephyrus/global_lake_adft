"""Job specification and runtime record models.

``JobSpec`` captures *what* to do; ``JobRecord`` extends it with *where we are*
at runtime.  Both are fully JSON-serialisable so recovery across process
restarts requires no pickle or in-memory object reconstruction.
"""

from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass, field  # noqa: F401 (field used in WriteParams)
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


class JobState(str, Enum):
    """Ordered states of a hydrofetch export job."""

    HOLD = "hold"
    EXPORT = "export"
    DOWNLOAD = "download"
    CLEANUP = "cleanup"
    SAMPLE = "sample"
    WRITE = "write"
    COMPLETED = "completed"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        """Return True for states that will never advance further."""
        return self in (JobState.COMPLETED, JobState.FAILED)

    @property
    def is_active(self) -> bool:
        """Return True for states that hold a concurrency slot."""
        return self not in (JobState.HOLD, JobState.COMPLETED, JobState.FAILED)


# ---------------------------------------------------------------------------
# GEE export parameters
# ---------------------------------------------------------------------------


@dataclass
class GeeExportParams:
    """Parameters for the GEE Export.image.toDrive step."""

    spec_id: str
    asset_id: str
    bands: list[str]
    scale: float
    crs: str
    max_pixels: int
    region_geojson: dict[str, Any]
    drive_folder: str | None = None


# ---------------------------------------------------------------------------
# Local sampling parameters
# ---------------------------------------------------------------------------


@dataclass
class SampleParams:
    """Parameters for the local raster sampling step.

    Args:
        geometry_path: Path to a CSV or GeoJSON file with lake geometries /
            centroids.  CSV must have columns [``id_column``, ``lon``, ``lat``].
        id_column: Name of the lake identifier column (default ``hylak_id``).
    """

    geometry_path: str
    id_column: str = "hylak_id"


# ---------------------------------------------------------------------------
# Output write parameters
# ---------------------------------------------------------------------------


@dataclass
class WriteParams:
    """Parameters for the write step.

    Attributes:
        output_dir: Local directory for file-based sinks.  May be empty when
            ``"file"`` is not included in *sinks*.
        output_format: ``"parquet"`` or ``"csv"`` (file sink only).
        sinks: Ordered list of sink identifiers.  Supported values:
            ``"file"`` – copy sampled Parquet/CSV to *output_dir*;
            ``"db"``   – upsert rows into a PostgreSQL table.
            Defaults to ``["file"]``.
        db_table: Target PostgreSQL table name for the ``"db"`` sink
            (default ``"era5_forcing"``).
    """

    output_dir: str
    output_format: str = "parquet"  # "parquet" | "csv"
    sinks: list = field(default_factory=lambda: ["file"])
    db_table: str = "era5_forcing"


# ---------------------------------------------------------------------------
# Complete job specification
# ---------------------------------------------------------------------------


@dataclass
class JobSpec:
    """Immutable description of one hydrofetch export-sample-write job."""

    job_id: str
    export_name: str
    date_iso: str  # YYYY-MM-DD of the single-day image
    gee: GeeExportParams
    sample: SampleParams
    write: WriteParams


# ---------------------------------------------------------------------------
# Runtime job record (spec + mutable state)
# ---------------------------------------------------------------------------


@dataclass
class JobRecord:
    """Persisted runtime state for one job.  Mutate via :func:`update_state`."""

    spec: JobSpec
    state: JobState = JobState.HOLD

    # fields populated as the job progresses
    task_id: str | None = None         # set after GEE task is submitted
    drive_file_id: str | None = None   # set when the Drive file is located
    local_raw_path: str | None = None  # set after download
    local_sample_path: str | None = None  # set after sampling

    attempt: int = 0
    max_attempts: int = 3
    last_error: str | None = None

    created_at: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )
    updated_at: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )

    def advance(self, next_state: JobState, **kwargs: Any) -> "JobRecord":
        """Return a copy of this record with *next_state* and any extra field updates."""
        updates: dict[str, Any] = {
            "state": next_state,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
            **kwargs,
        }
        return dataclasses.replace(self, **updates)

    def fail(self, error: str) -> "JobRecord":
        """Return a copy with state FAILED and incremented attempt counter."""
        new_attempt = self.attempt + 1
        new_state = (
            JobState.FAILED if new_attempt >= self.max_attempts else self.state
        )
        return dataclasses.replace(
            self,
            state=new_state,
            attempt=new_attempt,
            last_error=error,
            updated_at=datetime.now(tz=timezone.utc).isoformat(),
        )


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------


def _spec_to_dict(spec: JobSpec) -> dict[str, Any]:
    return {
        "job_id": spec.job_id,
        "export_name": spec.export_name,
        "date_iso": spec.date_iso,
        "gee": dataclasses.asdict(spec.gee),
        "sample": dataclasses.asdict(spec.sample),
        "write": dataclasses.asdict(spec.write),
    }


def _spec_from_dict(data: dict[str, Any]) -> JobSpec:
    return JobSpec(
        job_id=data["job_id"],
        export_name=data["export_name"],
        date_iso=data["date_iso"],
        gee=GeeExportParams(**data["gee"]),
        sample=SampleParams(**data["sample"]),
        write=WriteParams(**data["write"]),
    )


def record_to_dict(record: JobRecord) -> dict[str, Any]:
    """Serialise a :class:`JobRecord` to a JSON-safe dict."""
    return {
        "spec": _spec_to_dict(record.spec),
        "state": record.state.value,
        "task_id": record.task_id,
        "drive_file_id": record.drive_file_id,
        "local_raw_path": record.local_raw_path,
        "local_sample_path": record.local_sample_path,
        "attempt": record.attempt,
        "max_attempts": record.max_attempts,
        "last_error": record.last_error,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def record_from_dict(data: dict[str, Any]) -> JobRecord:
    """Deserialise a :class:`JobRecord` from a dict (as produced by :func:`record_to_dict`)."""
    return JobRecord(
        spec=_spec_from_dict(data["spec"]),
        state=JobState(data["state"]),
        task_id=data.get("task_id"),
        drive_file_id=data.get("drive_file_id"),
        local_raw_path=data.get("local_raw_path"),
        local_sample_path=data.get("local_sample_path"),
        attempt=data.get("attempt", 0),
        max_attempts=data.get("max_attempts", 3),
        last_error=data.get("last_error"),
        created_at=data.get("created_at", ""),
        updated_at=data.get("updated_at", ""),
    )


def record_to_json(record: JobRecord) -> str:
    """Serialise *record* to a JSON string."""
    return json.dumps(record_to_dict(record), indent=2)


def record_from_json(text: str) -> JobRecord:
    """Deserialise a :class:`JobRecord` from a JSON string."""
    return record_from_dict(json.loads(text))


def record_from_file(path: Path) -> JobRecord:
    """Load a :class:`JobRecord` from a JSON file."""
    with path.open(encoding="utf-8") as fh:
        return record_from_dict(json.load(fh))


__all__ = [
    "GeeExportParams",
    "JobRecord",
    "JobSpec",
    "JobState",
    "SampleParams",
    "WriteParams",
    "record_from_dict",
    "record_from_file",
    "record_from_json",
    "record_to_dict",
    "record_to_json",
]
