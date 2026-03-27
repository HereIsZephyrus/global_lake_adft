"""FastAPI route definitions for all dashboard endpoints."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from hydrofetch_dashboard_api import config
from hydrofetch_dashboard_api.services import metrics as svc
from hydrofetch_dashboard_api.sources import database as db_src
from hydrofetch_dashboard_api.sources import logs as log_src
from hydrofetch_dashboard_api.sources.jobs import load_jobs

router = APIRouter(prefix="/api")


def _jobs_df():
    try:
        return load_jobs(config.JOB_DIR).jobs_df
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("/health")
def health():
    return {"status": "ok", "job_dir": str(config.JOB_DIR), "db_table": config.DB_TABLE}


@router.get("/overview")
def overview():
    jobs_df = _jobs_df()
    return {
        **svc.kpis(jobs_df),
        "job_dir": str(config.JOB_DIR),
        "log_dir": str(config.LOG_DIR),
        "db_table": config.DB_TABLE,
    }


@router.get("/states")
def states():
    return svc.state_counts(_jobs_df())


@router.get("/timeline")
def timeline_route(hours: Annotated[int, Query(ge=1, le=168)] = 6):
    return svc.timeline(_jobs_df(), hours=hours)


@router.get("/tile-progress")
def tile_progress():
    return svc.tile_progress(_jobs_df())


@router.get("/date-progress")
def date_progress():
    return svc.date_progress(_jobs_df())


@router.get("/failures")
def failures_route():
    rows, summary = svc.failures(_jobs_df())
    return {"items": rows, "summary": summary}


@router.get("/jobs")
def jobs_route(
    state: str | None = None,
    tile_id: str | None = None,
    min_attempt: int = 0,
    limit: Annotated[int, Query(ge=1, le=2000)] = 200,
    offset: int = 0,
):
    return svc.jobs_page(
        _jobs_df(),
        state=state,
        tile_id=tile_id,
        min_attempt=min_attempt,
        limit=limit,
        offset=offset,
    )


@router.get("/ingest")
def ingest():
    return db_src.load_ingest_stats(config.DB_TABLE)


@router.get("/alerts")
def alerts_route():
    return svc.alerts(_jobs_df())


@router.get("/logs")
def logs_route():
    return log_src.log_error_summary(config.LOG_DIR)
