"""Build dashboard metrics from normalized job DataFrames."""

from __future__ import annotations

import math
import re

import pandas as pd

from hydrofetch_dashboard_api.sources.jobs import STATE_ORDER

_WINDOW_RE = re.compile(r"Window\([^)]*\)")
_NUMBER_RE = re.compile(r"-?\d+(?:\.\d+)?")
_SPACE_RE = re.compile(r"\s+")


def normalize_error(msg: str | None) -> str:
    if not msg:
        return "unknown"
    t = _WINDOW_RE.sub("Window(...)", msg.strip())
    t = _NUMBER_RE.sub("<n>", t)
    return _SPACE_RE.sub(" ", t)[:240]


def kpis(jobs_df: pd.DataFrame) -> dict:
    total = len(jobs_df)
    if not total:
        return {
            "total_jobs": 0,
            "active_jobs": 0,
            "completed_jobs": 0,
            "failed_jobs": 0,
            "stalled_jobs": 0,
            "completion_rate": 0.0,
            "failure_rate": 0.0,
        }
    completed = int((jobs_df["state"] == "completed").sum())
    failed = int((jobs_df["state"] == "failed").sum())
    active = int(jobs_df["is_active"].sum())
    stalled = int((jobs_df["is_active"] & (jobs_df["updated_age_hours"] >= 1)).sum())
    return {
        "total_jobs": total,
        "active_jobs": active,
        "completed_jobs": completed,
        "failed_jobs": failed,
        "stalled_jobs": stalled,
        "completion_rate": round(completed / total * 100, 4),
        "failure_rate": round(failed / total * 100, 4),
    }


def state_counts(jobs_df: pd.DataFrame) -> list[dict]:
    if jobs_df.empty:
        return [{"state": s, "count": 0} for s in STATE_ORDER]
    counts = (
        jobs_df.groupby("state", observed=False)
        .size()
        .reindex(STATE_ORDER, fill_value=0)
    )
    return [{"state": str(s), "count": int(c)} for s, c in counts.items()]


def timeline(jobs_df: pd.DataFrame, hours: int = 6) -> list[dict]:
    if jobs_df.empty:
        return []
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=hours)
    recent = jobs_df.loc[jobs_df["updated_ts"] >= cutoff, ["updated_ts", "state"]].copy()
    if recent.empty:
        return []
    recent["hour"] = recent["updated_ts"].dt.floor("h")
    grouped = (
        recent.groupby(["hour", "state"], observed=False)
        .size()
        .reset_index(name="count")
        .sort_values(["hour", "state"])
    )
    return [
        {
            "hour": row["hour"].isoformat() if hasattr(row["hour"], "isoformat") else str(row["hour"]),
            "state": str(row["state"]),
            "count": int(row["count"]),
        }
        for _, row in grouped.iterrows()
    ]


def failures(jobs_df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Return (failure_rows, error_summary)."""
    if jobs_df.empty:
        return [], []
    failed = jobs_df.loc[jobs_df["state"] == "failed"].copy()
    if failed.empty:
        return [], []
    failed["error_group"] = failed["last_error"].map(normalize_error)
    detail_cols = [
        "job_id", "tile_id", "date_iso", "attempt", "last_error",
        "error_group", "updated_at", "task_id",
    ]
    detail = failed[detail_cols].copy()
    detail = detail.where(detail.notna(), other=None)
    rows = detail.to_dict("records")

    error_counts = (
        failed.groupby("error_group")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )
    summary = [
        {"error_group": str(r["error_group"]), "count": int(r["count"])}
        for _, r in error_counts.iterrows()
    ]
    return rows, summary


def tile_progress(jobs_df: pd.DataFrame) -> list[dict]:
    if jobs_df.empty:
        return []
    grouped = jobs_df.groupby("tile_id", dropna=False)
    progress = grouped.agg(
        total=("job_id", "size"),
        completed=("state", lambda s: int((s == "completed").sum())),
        failed=("state", lambda s: int((s == "failed").sum())),
        active=("is_active", "sum"),
        pending=("state", lambda s: int((s == "hold").sum())),
    ).reset_index()
    progress = progress.sort_values(["failed", "active", "tile_id"], ascending=[False, False, True])
    return progress.where(progress.notna(), other=None).to_dict("records")


def date_progress(jobs_df: pd.DataFrame) -> list[dict]:
    if jobs_df.empty:
        return []
    grouped = jobs_df.groupby("date_iso", dropna=False)
    progress = grouped.agg(
        total=("job_id", "size"),
        completed=("state", lambda s: int((s == "completed").sum())),
        failed=("state", lambda s: int((s == "failed").sum())),
        active=("is_active", "sum"),
    ).reset_index()
    return progress.sort_values("date_iso", ascending=False).to_dict("records")


def alerts(jobs_df: pd.DataFrame) -> dict:
    if jobs_df.empty:
        return {"stalled": [], "high_retry": [], "write_waiting": []}

    stalled_cols = ["job_id", "state", "tile_id", "date_iso", "updated_at", "updated_age_hours"]
    stalled = (
        jobs_df.loc[jobs_df["is_active"] & (jobs_df["updated_age_hours"] >= 1), stalled_cols]
        .sort_values("updated_age_hours", ascending=False)
        .where(lambda df: df.notna(), other=None)
        .to_dict("records")
    )

    retry_cols = ["job_id", "state", "tile_id", "date_iso", "attempt", "updated_at"]
    high_retry = (
        jobs_df.loc[(jobs_df["attempt"] >= 2) & (jobs_df["state"] != "failed"), retry_cols]
        .sort_values(["attempt"], ascending=False)
        .where(lambda df: df.notna(), other=None)
        .to_dict("records")
    )

    ww_cols = ["job_id", "tile_id", "date_iso", "state", "updated_at"]
    write_waiting = (
        jobs_df.loc[
            jobs_df["state"].isin(["write", "completed"]) & jobs_df["local_sample_path"].isna(),
            ww_cols,
        ]
        .where(lambda df: df.notna(), other=None)
        .to_dict("records")
    )

    return {"stalled": stalled, "high_retry": high_retry, "write_waiting": write_waiting}


def jobs_page(
    jobs_df: pd.DataFrame,
    *,
    state: str | None = None,
    tile_id: str | None = None,
    min_attempt: int = 0,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    filtered = jobs_df.copy()
    if state:
        filtered = filtered.loc[filtered["state"].astype(str) == state]
    if tile_id:
        filtered = filtered.loc[filtered["tile_id"].astype(str) == tile_id]
    filtered = filtered.loc[filtered["attempt"] >= min_attempt]
    total = len(filtered)

    cols = [
        "job_id", "state", "tile_id", "date_iso", "attempt",
        "task_id", "drive_file_id", "local_raw_path", "local_sample_path",
        "updated_at", "last_error",
    ]
    page = filtered[cols].iloc[offset : offset + limit].copy()
    page["state"] = page["state"].astype(str)
    items = page.where(page.notna(), other=None).to_dict("records")
    return {"items": items, "total": total, "limit": limit, "offset": offset}


__all__ = [
    "alerts",
    "date_progress",
    "failures",
    "jobs_page",
    "kpis",
    "normalize_error",
    "state_counts",
    "tile_progress",
    "timeline",
]
