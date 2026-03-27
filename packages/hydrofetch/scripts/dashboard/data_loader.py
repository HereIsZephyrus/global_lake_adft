"""Load and normalize Hydrofetch job JSON files for the local dashboard."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

STATE_ORDER = [
    "hold",
    "export",
    "download",
    "cleanup",
    "sample",
    "write",
    "completed",
    "failed",
]


@dataclass(slots=True)
class LoadResult:
    """Normalized dashboard data derived from a job directory."""

    jobs_df: pd.DataFrame
    job_dir: Path
    total_files: int
    invalid_files: list[str]


def _flatten_job(payload: dict[str, Any], source_path: Path) -> dict[str, Any]:
    spec = payload.get("spec", {})
    gee = spec.get("gee", {})
    sample = spec.get("sample", {})
    write = spec.get("write", {})
    state = str(payload.get("state") or "unknown")
    sinks = write.get("sinks") or []
    row = {
        "job_id": spec.get("job_id"),
        "export_name": spec.get("export_name"),
        "date_iso": spec.get("date_iso"),
        "tile_id": sample.get("tile_id") or gee.get("tile_id"),
        "state": state,
        "task_id": payload.get("task_id"),
        "drive_file_id": payload.get("drive_file_id"),
        "local_raw_path": payload.get("local_raw_path"),
        "local_sample_path": payload.get("local_sample_path"),
        "geometry_path": sample.get("geometry_path"),
        "db_table": write.get("db_table"),
        "sinks": ", ".join(str(item) for item in sinks),
        "attempt": int(payload.get("attempt") or 0),
        "max_attempts": int(payload.get("max_attempts") or 0),
        "last_error": payload.get("last_error"),
        "created_at": payload.get("created_at"),
        "updated_at": payload.get("updated_at"),
        "source_path": str(source_path),
        "is_terminal": state in {"completed", "failed"},
        "is_active": state not in {"hold", "completed", "failed"},
        "has_error": bool(payload.get("last_error")),
    }
    return row


def _empty_jobs_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "job_id",
            "export_name",
            "date_iso",
            "tile_id",
            "state",
            "task_id",
            "drive_file_id",
            "local_raw_path",
            "local_sample_path",
            "geometry_path",
            "db_table",
            "sinks",
            "attempt",
            "max_attempts",
            "last_error",
            "created_at",
            "updated_at",
            "source_path",
            "is_terminal",
            "is_active",
            "has_error",
            "date",
            "created_ts",
            "updated_ts",
            "updated_age_hours",
        ]
    )


def load_jobs(job_dir: str | Path) -> LoadResult:
    """Read all Hydrofetch job JSON files from *job_dir* into a DataFrame."""

    job_dir_path = Path(job_dir).expanduser().resolve()
    if not job_dir_path.exists():
        raise FileNotFoundError(f"Job directory does not exist: {job_dir_path}")

    rows: list[dict[str, Any]] = []
    invalid_files: list[str] = []
    files = sorted(job_dir_path.glob("*.json"))
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows.append(_flatten_job(payload, path))
        except Exception:
            invalid_files.append(str(path))

    if not rows:
        return LoadResult(
            jobs_df=_empty_jobs_df(),
            job_dir=job_dir_path,
            total_files=len(files),
            invalid_files=invalid_files,
        )

    jobs_df = pd.DataFrame(rows)
    jobs_df["date"] = pd.to_datetime(jobs_df["date_iso"], errors="coerce")
    jobs_df["created_ts"] = pd.to_datetime(jobs_df["created_at"], utc=True, errors="coerce")
    jobs_df["updated_ts"] = pd.to_datetime(jobs_df["updated_at"], utc=True, errors="coerce")
    now = pd.Timestamp.utcnow()
    jobs_df["updated_age_hours"] = (
        (now - jobs_df["updated_ts"]).dt.total_seconds() / 3600.0
    ).round(2)
    jobs_df["state"] = pd.Categorical(
        jobs_df["state"],
        categories=STATE_ORDER,
        ordered=True,
    )
    jobs_df = jobs_df.sort_values(["updated_ts", "job_id"], ascending=[False, True]).reset_index(
        drop=True
    )
    return LoadResult(
        jobs_df=jobs_df,
        job_dir=job_dir_path,
        total_files=len(files),
        invalid_files=invalid_files,
    )


__all__ = ["LoadResult", "STATE_ORDER", "load_jobs"]
