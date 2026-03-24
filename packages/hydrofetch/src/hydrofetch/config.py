"""Environment configuration loading for hydrofetch.

All runtime settings are read from environment variables (optionally via a .env
file).  Every variable is prefixed with ``HYDROFETCH_`` to avoid collisions.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_dotenv_loaded: bool = False


def load_env(env_file: str | Path | None = None) -> None:
    """Load .env file once.  Pass ``env_file`` to override the default location."""
    global _dotenv_loaded  # pylint: disable=global-statement
    if not _dotenv_loaded:
        if env_file is not None:
            load_dotenv(Path(env_file).expanduser().resolve())
        else:
            load_dotenv()
        _dotenv_loaded = True


def _require(key: str) -> str:
    """Return the value of an environment variable or raise ``ValueError``."""
    value = os.environ.get(key, "").strip()
    if not value:
        raise ValueError(
            f"Required environment variable {key!r} is not set. "
            "Add it to your .env file or export it before running hydrofetch."
        )
    return value


def get_gee_project() -> str:
    """GEE cloud project ID (``HYDROFETCH_GEE_PROJECT``)."""
    load_env()
    return _require("HYDROFETCH_GEE_PROJECT")


def get_credentials_file() -> Path:
    """Path to the Google API OAuth2 client-secrets JSON (``HYDROFETCH_CREDENTIALS_FILE``)."""
    load_env()
    raw = _require("HYDROFETCH_CREDENTIALS_FILE")
    path = Path(raw).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(
            f"HYDROFETCH_CREDENTIALS_FILE does not exist: {path}\n"
            "Download OAuth client credentials from Google Cloud Console."
        )
    return path


def get_token_file() -> Path:
    """Path for the saved OAuth2 token JSON (``HYDROFETCH_TOKEN_FILE``).

    The file is created automatically after the first browser-based consent flow.
    """
    load_env()
    raw = os.environ.get("HYDROFETCH_TOKEN_FILE", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path.home() / ".hydrofetch" / "token.json"


def get_drive_folder_name() -> str | None:
    """Google Drive folder name for GEE exports (``HYDROFETCH_DRIVE_FOLDER_NAME``).

    Returns ``None`` when unset (GEE uses the default root).
    """
    load_env()
    raw = os.environ.get("HYDROFETCH_DRIVE_FOLDER_NAME", "").strip()
    return raw or None


def get_job_dir() -> Path:
    """Directory that stores serialised job records (``HYDROFETCH_JOB_DIR``)."""
    load_env()
    raw = os.environ.get("HYDROFETCH_JOB_DIR", "").strip()
    path = Path(raw).expanduser().resolve() if raw else Path.cwd() / "hydrofetch_jobs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_raw_dir() -> Path:
    """Directory for downloaded raw GeoTIFF files (``HYDROFETCH_RAW_DIR``)."""
    load_env()
    raw = os.environ.get("HYDROFETCH_RAW_DIR", "").strip()
    path = Path(raw).expanduser().resolve() if raw else Path.cwd() / "hydrofetch_raw"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_sample_dir() -> Path:
    """Directory for sampled lake-forcing outputs (``HYDROFETCH_SAMPLE_DIR``)."""
    load_env()
    raw = os.environ.get("HYDROFETCH_SAMPLE_DIR", "").strip()
    path = Path(raw).expanduser().resolve() if raw else Path.cwd() / "hydrofetch_sample"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_max_concurrent() -> int:
    """Maximum concurrent GEE export tasks (``HYDROFETCH_MAX_CONCURRENT``, default 5)."""
    load_env()
    raw = os.environ.get("HYDROFETCH_MAX_CONCURRENT", "5").strip()
    try:
        value = int(raw)
    except ValueError as err:
        raise ValueError(
            f"HYDROFETCH_MAX_CONCURRENT must be an integer, got {raw!r}"
        ) from err
    if value < 1:
        raise ValueError("HYDROFETCH_MAX_CONCURRENT must be >= 1")
    return value


def get_poll_interval() -> float:
    """Seconds between status-check polls (``HYDROFETCH_POLL_INTERVAL``, default 15)."""
    load_env()
    raw = os.environ.get("HYDROFETCH_POLL_INTERVAL", "15").strip()
    try:
        value = float(raw)
    except ValueError as err:
        raise ValueError(
            f"HYDROFETCH_POLL_INTERVAL must be a number, got {raw!r}"
        ) from err
    if value <= 0:
        raise ValueError("HYDROFETCH_POLL_INTERVAL must be positive")
    return value


# ---------------------------------------------------------------------------
# PostgreSQL / DB sink
# ---------------------------------------------------------------------------


def get_db_params() -> dict:
    """Return a dict of psycopg connection keyword arguments.

    Reads the following environment variables:

    * ``HYDROFETCH_DB_HOST``     – default ``"localhost"``
    * ``HYDROFETCH_DB_PORT``     – default ``5432``
    * ``HYDROFETCH_DB``     – **required**
    * ``HYDROFETCH_DB_USER``     – **required**
    * ``HYDROFETCH_DB_PASSWORD`` – **required**

    Raises:
        ValueError: If any required variable is missing or ``DB_PORT`` is not
            an integer.
    """
    load_env()

    dbname = _require("HYDROFETCH_DB")
    user = _require("HYDROFETCH_DB_USER")
    password = _require("HYDROFETCH_DB_PASSWORD")

    host = os.environ.get("HYDROFETCH_DB_HOST", "localhost").strip() or "localhost"

    port_raw = os.environ.get("HYDROFETCH_DB_PORT", "5432").strip()
    try:
        port = int(port_raw)
    except ValueError as err:
        raise ValueError(
            f"HYDROFETCH_DB_PORT must be an integer, got {port_raw!r}"
        ) from err

    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


__all__ = [
    "load_env",
    "get_gee_project",
    "get_credentials_file",
    "get_token_file",
    "get_drive_folder_name",
    "get_job_dir",
    "get_raw_dir",
    "get_sample_dir",
    "get_max_concurrent",
    "get_poll_interval",
    "get_db_params",
]
