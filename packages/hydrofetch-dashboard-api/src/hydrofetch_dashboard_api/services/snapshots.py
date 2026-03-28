"""Periodic state-count snapshots for dashboard timeline charts."""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path

import pandas as pd

from hydrofetch_dashboard_api.config import (
    JOB_DIR,
    PROJECTS_DIR,
    SNAPSHOT_INTERVAL_SECONDS,
    SNAPSHOT_RETENTION_HOURS,
)
from hydrofetch_dashboard_api.sources.jobs import STATE_ORDER, load_jobs
from hydrofetch_dashboard_api.sources.projects import list_projects, resolve_paths

log = logging.getLogger(__name__)

SNAPSHOT_FILENAME = ".dashboard_state_snapshots.jsonl"
_FILE_LOCK = threading.Lock()
_LAST_PRUNED_AT: dict[str, float] = {}


def snapshot_path(job_dir: str | Path) -> Path:
    """Return the hidden JSONL file used to store timeline snapshots."""
    return Path(job_dir).expanduser().resolve() / SNAPSHOT_FILENAME


def _utc_now() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")


def _empty_counts() -> dict[str, int]:
    return {state: 0 for state in STATE_ORDER}


def _state_count_map(jobs_df: pd.DataFrame) -> dict[str, int]:
    if jobs_df.empty:
        return _empty_counts()
    counts = (
        jobs_df.groupby("state", observed=False)
        .size()
        .reindex(STATE_ORDER, fill_value=0)
    )
    return {str(state): int(count) for state, count in counts.items()}


def _parse_snapshot_line(line: str) -> dict | None:
    raw = line.strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    ts = pd.to_datetime(payload.get("ts"), utc=True, errors="coerce")
    if pd.isna(ts):
        return None
    counts_raw = payload.get("counts") or {}
    counts = {
        state: int(counts_raw.get(state, 0) or 0)
        for state in STATE_ORDER
    }
    return {"ts": ts, "counts": counts}


def _read_snapshots(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    results: list[dict] = []
    with _FILE_LOCK:
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                parsed = _parse_snapshot_line(line)
                if parsed is not None:
                    results.append(parsed)
    results.sort(key=lambda item: item["ts"])
    return results


def _prune_snapshot_file(path: Path, *, retain_hours: int) -> None:
    cutoff = _utc_now() - pd.Timedelta(hours=retain_hours)
    kept_lines: list[str] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            parsed = _parse_snapshot_line(line)
            if parsed is not None and parsed["ts"] >= cutoff:
                kept_lines.append(
                    json.dumps(
                        {
                            "ts": parsed["ts"].isoformat(),
                            "counts": parsed["counts"],
                        },
                        ensure_ascii=False,
                    )
                )
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as fh:
        for line in kept_lines:
            fh.write(line + "\n")
    tmp_path.replace(path)


def record_snapshot(
    job_dir: str | Path,
    *,
    jobs_df: pd.DataFrame | None = None,
    recorded_at: pd.Timestamp | None = None,
) -> dict:
    """Append one snapshot row to disk and return the payload."""
    job_dir_path = Path(job_dir).expanduser().resolve()
    job_dir_path.mkdir(parents=True, exist_ok=True)
    ts = recorded_at if recorded_at is not None else _utc_now()
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    counts = _state_count_map(jobs_df if jobs_df is not None else load_jobs(job_dir_path).jobs_df)
    payload = {"ts": ts.isoformat(), "counts": counts}
    path = snapshot_path(job_dir_path)
    with _FILE_LOCK:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        now_monotonic = time.monotonic()
        last_pruned = _LAST_PRUNED_AT.get(str(path), 0.0)
        if now_monotonic - last_pruned >= 3600:
            _prune_snapshot_file(path, retain_hours=SNAPSHOT_RETENTION_HOURS)
            _LAST_PRUNED_AT[str(path)] = now_monotonic
    return payload


def timeline(job_dir: str | Path, hours: int = 6) -> list[dict]:
    """Return timeline points derived from saved snapshots."""
    bucket = "10min"
    now_ts = _utc_now()
    bucket_delta = pd.Timedelta(bucket)
    now_bucket = now_ts.floor(bucket)
    cutoff = now_bucket - pd.Timedelta(hours=hours)
    path = snapshot_path(job_dir)
    snapshots = _read_snapshots(path)

    if not snapshots:
        try:
            record_snapshot(job_dir, recorded_at=now_ts)
        except FileNotFoundError:
            return []
        snapshots = _read_snapshots(path)
    if not snapshots:
        return []

    seed = None
    recent: list[dict] = []
    for item in snapshots:
        if item["ts"] < cutoff:
            seed = item
        else:
            recent.append(item)
    selected = ([seed] if seed is not None else []) + recent
    if not selected:
        return []

    rows = [{"ts": item["ts"], **item["counts"]} for item in selected]
    frame = pd.DataFrame(rows).sort_values("ts")
    hours_index = pd.date_range(start=cutoff, end=now_bucket, freq=bucket, tz="UTC")
    query_frame = pd.DataFrame({"hour": hours_index})
    query_frame["query_ts"] = query_frame["hour"] + bucket_delta
    query_frame["query_ts"] = query_frame["query_ts"].where(query_frame["query_ts"] <= now_ts, now_ts)
    aligned = pd.merge_asof(
        query_frame.sort_values("query_ts"),
        frame.sort_values("ts"),
        left_on="query_ts",
        right_on="ts",
        direction="backward",
    )
    for state in STATE_ORDER:
        aligned[state] = aligned[state].fillna(0).astype(int)

    points: list[dict] = []
    for _, row in aligned.iterrows():
        hour = row["hour"]
        for state in STATE_ORDER:
            points.append(
                {
                    "hour": hour.isoformat() if hasattr(hour, "isoformat") else str(hour),
                    "state": state,
                    "count": int(row[state]),
                }
            )
    return points


class SnapshotManager:
    """Background thread that periodically snapshots project state counts."""

    def __init__(self, interval_seconds: int) -> None:
        self._interval_seconds = max(10, int(interval_seconds))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="dashboard-snapshots", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None

    def capture_once(self) -> None:
        targets: list[tuple[str, Path]] = []
        if JOB_DIR.exists():
            targets.append(("legacy", JOB_DIR))
        for project in list_projects(PROJECTS_DIR):
            paths = resolve_paths(PROJECTS_DIR, project.project_id)
            targets.append((project.project_id, paths["job_dir"]))

        for target_name, job_dir in targets:
            try:
                record_snapshot(job_dir)
            except FileNotFoundError:
                continue
            except Exception:
                log.exception("Failed to capture snapshot for %s", target_name)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.capture_once()
            if self._stop_event.wait(self._interval_seconds):
                break

manager = SnapshotManager(SNAPSHOT_INTERVAL_SECONDS)

__all__ = [
    "SNAPSHOT_FILENAME",
    "SnapshotManager",
    "manager",
    "record_snapshot",
    "snapshot_path",
    "timeline",
]
