"""Runtime configuration from environment variables."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parents[4]

# Load hydrofetch's .env as the primary config source
load_dotenv(_REPO_ROOT / "packages" / "hydrofetch" / ".env")


def _path_env(key: str, default: Path) -> Path:
    raw = os.getenv(key, "").strip()
    return Path(raw).expanduser().resolve() if raw else default


JOB_DIR: Path = _path_env(
    "HYDROFETCH_DASHBOARD_JOB_DIR",
    _REPO_ROOT / "data" / "hydrofetch_full_file_db_jobs",
)
LOG_DIR: Path = _path_env(
    "HYDROFETCH_DASHBOARD_LOG_DIR",
    _REPO_ROOT / "logs",
)
DB_TABLE: str = os.getenv("HYDROFETCH_DASHBOARD_DB_TABLE", "era5_forcing")
API_PORT: int = int(os.getenv("HYDROFETCH_DASHBOARD_API_PORT", "8050"))
CORS_ORIGINS: list[str] = ["http://localhost:5170", "http://127.0.0.1:5170"]
