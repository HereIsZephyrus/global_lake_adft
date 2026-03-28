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
DB_SIZE_TOTAL_REFRESH_SECONDS: int = int(
    os.getenv("HYDROFETCH_DASHBOARD_DB_SIZE_TOTAL_REFRESH_SECONDS", "7200")
)
DB_SIZE_TABLE_REFRESH_SECONDS: int = int(
    os.getenv("HYDROFETCH_DASHBOARD_DB_SIZE_TABLE_REFRESH_SECONDS", "1800")
)
SNAPSHOT_INTERVAL_SECONDS: int = int(
    os.getenv("HYDROFETCH_DASHBOARD_SNAPSHOT_INTERVAL_SECONDS", "60")
)
SNAPSHOT_RETENTION_HOURS: int = int(
    os.getenv("HYDROFETCH_DASHBOARD_SNAPSHOT_RETENTION_HOURS", "168")
)

# Multi-project support
PROJECTS_DIR: Path = _path_env(
    "HYDROFETCH_DASHBOARD_PROJECTS_DIR",
    _REPO_ROOT / "data" / "projects",
)

# Tile manifest for dashboard-spawned ``hydrofetch era5 --tile-manifest``.
# Default: repo ``data/continents/continents_manifest.json`` when present.
_default_manifest = _REPO_ROOT / "data" / "continents" / "continents_manifest.json"
_manifest_raw = os.getenv("HYDROFETCH_TILE_MANIFEST", "").strip()
if _manifest_raw:
    TILE_MANIFEST: str = str(Path(_manifest_raw).expanduser().resolve())
elif _default_manifest.is_file():
    TILE_MANIFEST = str(_default_manifest.resolve())
else:
    TILE_MANIFEST = ""
