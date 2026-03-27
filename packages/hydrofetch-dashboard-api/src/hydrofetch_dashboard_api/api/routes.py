"""FastAPI route definitions for all dashboard endpoints."""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Body, HTTPException, Query

from hydrofetch_dashboard_api import config
from hydrofetch_dashboard_api.services import metrics as svc
from hydrofetch_dashboard_api.services.process_manager import manager as proc_manager
from hydrofetch_dashboard_api.sources import database as db_src
from hydrofetch_dashboard_api.sources import logs as log_src
from hydrofetch_dashboard_api.sources.jobs import load_jobs
from hydrofetch_dashboard_api.sources.projects import (
    ProjectConfig,
    create_project,
    delete_project,
    list_projects,
    load_project,
    resolve_paths,
)

router = APIRouter(prefix="/api")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _jobs_df(job_dir=None):
    """Load jobs DataFrame from *job_dir* (defaults to legacy config.JOB_DIR)."""
    target = job_dir or config.JOB_DIR
    try:
        return load_jobs(target).jobs_df
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


def _get_project(project_id: str) -> ProjectConfig:
    try:
        return load_project(config.PROJECTS_DIR, project_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


def _project_jobs_df(project_id: str):
    cfg = _get_project(project_id)
    paths = resolve_paths(config.PROJECTS_DIR, cfg.project_id)
    return _jobs_df(paths["job_dir"])


def _project_to_response(cfg: ProjectConfig) -> dict[str, Any]:
    paths = resolve_paths(config.PROJECTS_DIR, cfg.project_id)
    status = proc_manager.status(cfg.project_id)
    # Count active jobs from job dir (best-effort)
    active_jobs = 0
    try:
        df = load_jobs(paths["job_dir"]).jobs_df
        if "is_active" in df.columns:
            active_jobs = int(df["is_active"].sum())
    except Exception:
        pass
    return {
        **cfg.to_dict(),
        "status": status,
        "active_jobs": active_jobs,
    }


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------


@router.get("/health")
def health():
    return {"status": "ok", "job_dir": str(config.JOB_DIR), "db_table": config.DB_TABLE}


# ---------------------------------------------------------------------------
# Project CRUD
# ---------------------------------------------------------------------------


@router.get("/projects")
def get_projects():
    projects = list_projects(config.PROJECTS_DIR)
    return [_project_to_response(p) for p in projects]


@router.post("/projects", status_code=201)
def post_project(body: Annotated[dict, Body()]):
    required = ["project_name", "gee_project", "credentials_file", "start_date", "end_date"]
    missing = [k for k in required if not body.get(k)]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing fields: {missing}")
    try:
        cfg = create_project(config.PROJECTS_DIR, body)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _project_to_response(cfg)


@router.get("/projects/{project_id}")
def get_project(project_id: str):
    cfg = _get_project(project_id)
    return _project_to_response(cfg)


@router.delete("/projects/{project_id}", status_code=204)
def del_project(project_id: str):
    # Stop running process first
    proc_manager.stop(project_id)
    try:
        delete_project(config.PROJECTS_DIR, project_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# Process control
# ---------------------------------------------------------------------------


@router.post("/projects/{project_id}/start")
def start_project(project_id: str):
    cfg = _get_project(project_id)
    if not config.TILE_MANIFEST:
        raise HTTPException(
            status_code=503,
            detail="HYDROFETCH_TILE_MANIFEST environment variable is not set",
        )
    paths = resolve_paths(config.PROJECTS_DIR, project_id)
    try:
        proc_manager.start(
            project_id=project_id,
            gee_project=cfg.gee_project,
            credentials_file=cfg.credentials_file,
            start_date=cfg.start_date,
            end_date=cfg.end_date,
            tile_manifest=config.TILE_MANIFEST,
            job_dir=paths["job_dir"],
            raw_dir=paths["raw_dir"],
            sample_dir=paths["sample_dir"],
            log_dir=paths["log_dir"],
            max_concurrent=cfg.max_concurrent,
            db_table=config.DB_TABLE,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"status": "started", "project_id": project_id}


@router.post("/projects/{project_id}/stop")
def stop_project(project_id: str):
    _get_project(project_id)  # ensure project exists
    proc_manager.stop(project_id)
    return {"status": "stopped", "project_id": project_id}


# ---------------------------------------------------------------------------
# Per-project data endpoints
# ---------------------------------------------------------------------------


@router.get("/projects/{project_id}/overview")
def project_overview(project_id: str):
    cfg = _get_project(project_id)
    paths = resolve_paths(config.PROJECTS_DIR, project_id)
    jobs_df = _jobs_df(paths["job_dir"])
    return {
        **svc.kpis(jobs_df),
        "job_dir": str(paths["job_dir"]),
        "log_dir": str(paths["log_dir"]),
        "db_table": config.DB_TABLE,
        "project_name": cfg.project_name,
        "process_status": proc_manager.status(project_id),
    }


@router.get("/projects/{project_id}/states")
def project_states(project_id: str):
    return svc.state_counts(_project_jobs_df(project_id))


@router.get("/projects/{project_id}/timeline")
def project_timeline(
    project_id: str,
    hours: Annotated[int, Query(ge=1, le=168)] = 6,
):
    return svc.timeline(_project_jobs_df(project_id), hours=hours)


@router.get("/projects/{project_id}/failures")
def project_failures(project_id: str):
    rows, summary = svc.failures(_project_jobs_df(project_id))
    return {"items": rows, "summary": summary}


@router.get("/projects/{project_id}/jobs")
def project_jobs(
    project_id: str,
    state: str | None = None,
    tile_id: str | None = None,
    min_attempt: int = 0,
    limit: Annotated[int, Query(ge=1, le=2000)] = 200,
    offset: int = 0,
):
    return svc.jobs_page(
        _project_jobs_df(project_id),
        state=state,
        tile_id=tile_id,
        min_attempt=min_attempt,
        limit=limit,
        offset=offset,
    )


@router.get("/projects/{project_id}/alerts")
def project_alerts(project_id: str):
    return svc.alerts(_project_jobs_df(project_id))


@router.get("/projects/{project_id}/logs")
def project_logs(project_id: str):
    _get_project(project_id)
    paths = resolve_paths(config.PROJECTS_DIR, project_id)
    return log_src.log_error_summary(paths["log_dir"])


# ---------------------------------------------------------------------------
# Legacy (single-project) endpoints — preserved for backward compatibility
# ---------------------------------------------------------------------------


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


@router.get("/db-size")
def db_size():
    return db_src.load_db_size(table_names=[config.DB_TABLE])


@router.get("/alerts")
def alerts_route():
    return svc.alerts(_jobs_df())


@router.get("/logs")
def logs_route():
    return log_src.log_error_summary(config.LOG_DIR)
