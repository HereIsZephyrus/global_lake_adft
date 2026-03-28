"""FastAPI application entry point for the Hydrofetch Dashboard API."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hydrofetch_dashboard_api import config
from hydrofetch_dashboard_api.api.routes import router
from hydrofetch_dashboard_api.services.db_metrics import manager as db_metrics_manager
from hydrofetch_dashboard_api.services.process_manager import manager as proc_manager
from hydrofetch_dashboard_api.services.snapshots import manager as snapshot_manager

app = FastAPI(
    title="Hydrofetch Dashboard API",
    description="Monitoring and control API for Hydrofetch job status, failures and ingest progress.",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

app.include_router(router)


@app.on_event("startup")
async def startup_event() -> None:
    proc_manager.recover()
    db_metrics_manager.start()
    snapshot_manager.start()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    db_metrics_manager.stop()
    snapshot_manager.stop()


def main() -> None:
    uvicorn.run(
        "hydrofetch_dashboard_api.main:app",
        host="127.0.0.1",
        port=config.API_PORT,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
