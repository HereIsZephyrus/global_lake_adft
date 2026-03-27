"""Aggregate dashboard-friendly metrics from normalized Hydrofetch jobs."""

from __future__ import annotations

import math
import re

import pandas as pd

from data_loader import STATE_ORDER

_WINDOW_RE = re.compile(r"Window\([^)]*\)")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_SPACE_RE = re.compile(r"\s+")


def normalize_error_message(message: str | None) -> str:
    """Collapse noisy error strings into stable buckets for grouping."""

    if not message:
        return "unknown"
    text = message.strip()
    text = _WINDOW_RE.sub("Window(...)", text)
    text = _NUMBER_RE.sub("<n>", text)
    text = _SPACE_RE.sub(" ", text)
    return text[:240]


def build_kpis(jobs_df: pd.DataFrame) -> dict[str, float]:
    """Return top-level KPI values for the overview page."""

    total = int(len(jobs_df))
    active = int(jobs_df["is_active"].sum()) if total else 0
    completed = int((jobs_df["state"] == "completed").sum()) if total else 0
    failed = int((jobs_df["state"] == "failed").sum()) if total else 0
    stalled = int(((jobs_df["is_active"]) & (jobs_df["updated_age_hours"] >= 1)).sum()) if total else 0
    completion_rate = (completed / total * 100.0) if total else 0.0
    failure_rate = (failed / total * 100.0) if total else 0.0
    return {
        "total_jobs": total,
        "active_jobs": active,
        "completed_jobs": completed,
        "failed_jobs": failed,
        "stalled_jobs": stalled,
        "completion_rate": completion_rate,
        "failure_rate": failure_rate,
    }


def build_state_counts(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """Return ordered state counts."""

    if jobs_df.empty:
        return pd.DataFrame({"state": STATE_ORDER, "count": [0] * len(STATE_ORDER)})
    counts = (
        jobs_df.groupby("state", observed=False)
        .size()
        .reindex(STATE_ORDER, fill_value=0)
        .reset_index(name="count")
    )
    return counts


def build_recent_activity(jobs_df: pd.DataFrame, hours: int = 6) -> pd.DataFrame:
    """Approximate state movement trend using the latest update timestamp."""

    if jobs_df.empty:
        return pd.DataFrame(columns=["hour", "state", "count"])
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
    recent = jobs_df.loc[jobs_df["updated_ts"] >= cutoff, ["updated_ts", "state"]].copy()
    if recent.empty:
        return pd.DataFrame(columns=["hour", "state", "count"])
    recent["hour"] = recent["updated_ts"].dt.floor("h")
    return (
        recent.groupby(["hour", "state"], observed=False)
        .size()
        .reset_index(name="count")
        .sort_values(["hour", "state"])
    )


def build_failure_tables(jobs_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return failed job detail rows and grouped error counts."""

    if jobs_df.empty:
        empty = pd.DataFrame()
        return empty, empty
    failed_jobs = jobs_df.loc[jobs_df["state"] == "failed"].copy()
    if failed_jobs.empty:
        empty = pd.DataFrame()
        return failed_jobs, empty
    failed_jobs["error_group"] = failed_jobs["last_error"].map(normalize_error_message)
    error_counts = (
        failed_jobs.groupby("error_group")
        .size()
        .reset_index(name="count")
        .sort_values(["count", "error_group"], ascending=[False, True])
    )
    return failed_jobs, error_counts


def build_tile_progress(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """Return per-tile progress counts by key state groups."""

    if jobs_df.empty:
        return pd.DataFrame(columns=["tile_id", "total", "completed", "failed", "active", "pending"])
    grouped = jobs_df.groupby("tile_id", dropna=False)
    progress = grouped.agg(
        total=("job_id", "size"),
        completed=("state", lambda s: int((s == "completed").sum())),
        failed=("state", lambda s: int((s == "failed").sum())),
        active=("is_active", "sum"),
        pending=("state", lambda s: int((s == "hold").sum())),
    ).reset_index()
    return progress.sort_values(["failed", "active", "tile_id"], ascending=[False, False, True])


def build_date_progress(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """Return daily progress counts for completed, failed and active jobs."""

    if jobs_df.empty:
        return pd.DataFrame(columns=["date_iso", "completed", "failed", "active", "total"])
    grouped = jobs_df.groupby("date_iso", dropna=False)
    progress = grouped.agg(
        total=("job_id", "size"),
        completed=("state", lambda s: int((s == "completed").sum())),
        failed=("state", lambda s: int((s == "failed").sum())),
        active=("is_active", "sum"),
    ).reset_index()
    return progress.sort_values("date_iso", ascending=False)


def build_recent_updates(jobs_df: pd.DataFrame, limit: int = 50) -> pd.DataFrame:
    """Return the most recently updated jobs."""

    if jobs_df.empty:
        return jobs_df
    columns = [
        "job_id",
        "state",
        "tile_id",
        "date_iso",
        "attempt",
        "updated_at",
        "task_id",
        "drive_file_id",
        "local_raw_path",
        "local_sample_path",
        "last_error",
    ]
    return jobs_df.loc[:, columns].head(limit)


def build_alerts(jobs_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Return alert tables for the alert page."""

    if jobs_df.empty:
        empty = pd.DataFrame()
        return {"stalled": empty, "high_retry": empty, "write_waiting": empty}

    stalled = jobs_df.loc[(jobs_df["is_active"]) & (jobs_df["updated_age_hours"] >= 1)].copy()
    stalled = stalled.sort_values("updated_age_hours", ascending=False)

    high_retry = jobs_df.loc[(jobs_df["attempt"] >= 2) & (jobs_df["state"] != "failed")].copy()
    high_retry = high_retry.sort_values(["attempt", "updated_ts"], ascending=[False, False])

    write_waiting = jobs_df.loc[
        (jobs_df["state"].isin(["write", "completed"])) & (jobs_df["local_sample_path"].isna())
    ].copy()

    return {
        "stalled": stalled,
        "high_retry": high_retry,
        "write_waiting": write_waiting,
    }


def safe_percent(numerator: int | float, denominator: int | float) -> float:
    """Return a percentage guarded against division by zero."""

    if not denominator:
        return 0.0
    value = float(numerator) / float(denominator) * 100.0
    if math.isnan(value) or math.isinf(value):
        return 0.0
    return value


__all__ = [
    "build_alerts",
    "build_date_progress",
    "build_failure_tables",
    "build_kpis",
    "build_recent_activity",
    "build_recent_updates",
    "build_state_counts",
    "build_tile_progress",
    "normalize_error_message",
    "safe_percent",
]
