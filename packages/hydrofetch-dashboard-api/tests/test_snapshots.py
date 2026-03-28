from __future__ import annotations

import json

import pandas as pd

from hydrofetch_dashboard_api.services import snapshots
from hydrofetch_dashboard_api.sources.jobs import STATE_ORDER


def _jobs_df(**counts: int) -> pd.DataFrame:
    states: list[str] = []
    for state in STATE_ORDER:
        states.extend([state] * int(counts.get(state, 0)))
    return pd.DataFrame(
        {
            "state": pd.Categorical(states, categories=STATE_ORDER, ordered=True),
        }
    )


def _point_map(points: list[dict]) -> dict[tuple[str, str], int]:
    return {(item["hour"], item["state"]): item["count"] for item in points}


def test_timeline_uses_latest_snapshot_per_bucket_and_carries_forward(tmp_path, monkeypatch) -> None:
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()

    snapshots.record_snapshot(
        job_dir,
        jobs_df=_jobs_df(export=3, failed=1),
        recorded_at=pd.Timestamp("2026-03-28T00:05:00Z"),
    )
    snapshots.record_snapshot(
        job_dir,
        jobs_df=_jobs_df(export=1, failed=2),
        recorded_at=pd.Timestamp("2026-03-28T00:16:00Z"),
    )

    monkeypatch.setattr(snapshots, "_utc_now", lambda: pd.Timestamp("2026-03-28T00:24:00Z"))
    points = snapshots.timeline(job_dir, hours=1)
    lookup = _point_map(points)

    assert lookup[("2026-03-28T00:00:00+00:00", "export")] == 3
    assert lookup[("2026-03-28T00:00:00+00:00", "failed")] == 1
    assert lookup[("2026-03-28T00:10:00+00:00", "export")] == 1
    assert lookup[("2026-03-28T00:10:00+00:00", "failed")] == 2
    assert lookup[("2026-03-28T00:20:00+00:00", "export")] == 1
    assert lookup[("2026-03-28T00:20:00+00:00", "failed")] == 2


def test_timeline_bootstraps_snapshot_file_from_current_jobs(tmp_path, monkeypatch) -> None:
    job_dir = tmp_path / "jobs"
    job_dir.mkdir()
    (job_dir / "job-1.json").write_text(
        json.dumps(
            {
                "state": "download",
                "attempt": 0,
                "created_at": "2026-03-28T00:00:00Z",
                "updated_at": "2026-03-28T00:00:00Z",
                "spec": {
                    "job_id": "job-1",
                    "date_iso": "2026-03-28",
                    "gee": {},
                    "sample": {},
                    "write": {},
                },
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(snapshots, "_utc_now", lambda: pd.Timestamp("2026-03-28T00:09:00Z"))
    points = snapshots.timeline(job_dir, hours=1)

    assert snapshots.snapshot_path(job_dir).is_file()
    lookup = _point_map(points)
    assert lookup[("2026-03-28T00:00:00+00:00", "download")] == 1
