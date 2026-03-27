"""Read and normalize Hydrofetch job JSON files."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
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

_CACHE: dict[str, "_CacheEntry"] = {}
_CACHE_TTL_SECONDS = 10


@dataclass
class _CacheEntry:
    jobs_df: pd.DataFrame
    job_dir: Path
    total_files: int
    invalid_files: list[str]
    mtime: float
    loaded_at: float


@dataclass(slots=True)
class LoadResult:
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
    return {
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


def _dir_mtime(path: Path) -> float:
    try:
        return max((p.stat().st_mtime for p in path.glob("*.json")), default=0.0)
    except Exception:
        return 0.0


def load_jobs(job_dir: str | Path) -> LoadResult:
    """Read Hydrofetch job JSON files with short-TTL directory-mtime caching."""

    job_dir_path = Path(job_dir).expanduser().resolve()
    if not job_dir_path.exists():
        raise FileNotFoundError(f"Job directory does not exist: {job_dir_path}")

    cache_key = str(job_dir_path)
    now = time.monotonic()
    mtime = _dir_mtime(job_dir_path)
    entry = _CACHE.get(cache_key)
    if (
        entry is not None
        and (now - entry.loaded_at) < _CACHE_TTL_SECONDS
        and entry.mtime == mtime
    ):
        return LoadResult(
            jobs_df=entry.jobs_df,
            job_dir=entry.job_dir,
            total_files=entry.total_files,
            invalid_files=entry.invalid_files,
        )

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
        jobs_df = pd.DataFrame(columns=list(_flatten_job({}, Path()).keys()) + ["date", "created_ts", "updated_ts", "updated_age_hours"])
    else:
        jobs_df = pd.DataFrame(rows)
        jobs_df["date"] = pd.to_datetime(jobs_df["date_iso"], errors="coerce")
        jobs_df["created_ts"] = pd.to_datetime(jobs_df["created_at"], utc=True, errors="coerce")
        jobs_df["updated_ts"] = pd.to_datetime(jobs_df["updated_at"], utc=True, errors="coerce")
        now_ts = pd.Timestamp.utcnow()
        jobs_df["updated_age_hours"] = (
            (now_ts - jobs_df["updated_ts"]).dt.total_seconds() / 3600.0
        ).round(2)
        jobs_df["state"] = pd.Categorical(jobs_df["state"], categories=STATE_ORDER, ordered=True)
        jobs_df = jobs_df.sort_values(
            ["updated_ts", "job_id"], ascending=[False, True]
        ).reset_index(drop=True)

    _CACHE[cache_key] = _CacheEntry(
        jobs_df=jobs_df,
        job_dir=job_dir_path,
        total_files=len(files),
        invalid_files=invalid_files,
        mtime=mtime,
        loaded_at=time.monotonic(),
    )
    return LoadResult(
        jobs_df=jobs_df,
        job_dir=job_dir_path,
        total_files=len(files),
        invalid_files=invalid_files,
    )


__all__ = ["LoadResult", "STATE_ORDER", "load_jobs"]
