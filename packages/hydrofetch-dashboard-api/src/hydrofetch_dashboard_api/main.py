"""FastAPI application entry point for the Hydrofetch Dashboard API."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from hydrofetch_dashboard_api import config
from hydrofetch_dashboard_api.api.routes import router

app = FastAPI(
    title="Hydrofetch Dashboard API",
    description="Read-only monitoring API for Hydrofetch job status, failures and ingest progress.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=config.CORS_ORIGINS,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(router)


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
