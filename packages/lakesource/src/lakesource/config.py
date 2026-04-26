"""Data source configuration with backend selection and connection parameters."""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .table_config import TableConfig


class Backend(str, Enum):
    """Available data backends."""

    POSTGRES = "postgres"
    PARQUET = "parquet"


def _env(key: str, default: str | None = None) -> str | None:
    """Read an environment variable, stripping whitespace and quotes."""
    value = os.environ.get(key)
    if value is None:
        return default
    stripped = value.strip().strip('"')
    return stripped or default


@dataclass(frozen=True)
class SourceConfig:
    """Configuration for data access, including backend selection, DB connection,
    and time filtering.

    DB connection fields default from environment variables (loaded via
    ``.env`` / ``load_dotenv``).  They can also be passed explicitly.

    Attributes:
        backend: Which data backend to use (postgres or parquet).
        data_dir: Path to parquet files directory (required when backend=parquet).
        data_path: Path where raw water-balance datasets are stored / downloaded.
        workflow_version: Workflow version string for monthly transition data.
        year_start: Optional start year filter (None = no filter).
        year_end: Optional end year filter (None = no filter).
        tables: Table name mapping. None → auto-load from default config.toml.
        db_host: PostgreSQL host (default from DB_HOST env, or localhost).
        db_port: PostgreSQL port (default from DB_PORT env, or 5432).
        db_user: PostgreSQL user (default from DB_USER env).
        db_password: PostgreSQL password (default from DB_PASSWORD env).
        atlas_db_name: ALTAS_DB database name (default from ALTAS_DB env).
        series_db_name: SERIES_DB database name (default from SERIES_DB env).
    """

    backend: Backend | None = None
    data_dir: Path | None = None
    data_path: Path | None = None
    workflow_version: str = "monthly-transition-v1"
    year_start: int | None = None
    year_end: int | None = None
    tables: TableConfig | None = None
    db_host: str | None = None
    db_port: int | None = None
    db_user: str | None = None
    db_password: str | None = None
    atlas_db_name: str | None = None
    series_db_name: str | None = None

    def __post_init__(self) -> None:
        normalized = self.workflow_version.strip()
        if not normalized:
            raise ValueError("workflow_version must not be empty")
        object.__setattr__(self, "workflow_version", normalized)

        if self.backend is None:
            b = _env("DATA_BACKEND")
            if b:
                try:
                    object.__setattr__(self, "backend", Backend(b.lower()))
                except ValueError:
                    raise ValueError(f"Invalid DATA_BACKEND: {b!r}") from None
            else:
                object.__setattr__(self, "backend", Backend.POSTGRES)

        if self.backend == Backend.PARQUET and self.data_dir is None:
            d = _env("PARQUET_DATA_DIR")
            if d:
                object.__setattr__(self, "data_dir", Path(d))
            else:
                raise ValueError("data_dir is required when backend=parquet (set PARQUET_DATA_DIR)")

        if self.tables is None:
            object.__setattr__(self, "tables", TableConfig.default())

        if self.data_path is None:
            dp = _env("DATA_PATH")
            if dp:
                object.__setattr__(self, "data_path", Path(dp))

        if self.db_host is None:
            h = _env("DB_HOST")
            if h:
                object.__setattr__(self, "db_host", h)

        if self.db_port is None:
            p = _env("DB_PORT")
            if p:
                try:
                    object.__setattr__(self, "db_port", int(p))
                except ValueError:
                    raise ValueError(f"DB_PORT must be a valid integer, got {p!r}") from None

        if self.db_user is None:
            u = _env("DB_USER")
            if u:
                object.__setattr__(self, "db_user", u)

        if self.db_password is None:
            pw = _env("DB_PASSWORD")
            if pw:
                object.__setattr__(self, "db_password", pw)

        if self.atlas_db_name is None:
            a = _env("ALTAS_DB")
            if a:
                object.__setattr__(self, "atlas_db_name", a)

        if self.series_db_name is None:
            s = _env("SERIES_DB")
            if s:
                object.__setattr__(self, "series_db_name", s)

    @property
    def t(self) -> TableConfig:
        """Shortcut to the resolved TableConfig (never None after __post_init__)."""
        assert self.tables is not None
        return self.tables

    @property
    def resolved_db_host(self) -> str:
        """Resolved DB host with fallback to localhost."""
        return self.db_host or "localhost"

    @property
    def resolved_db_port(self) -> int:
        """Resolved DB port with fallback to 5432."""
        return self.db_port or 5432

    def connection_params(self, db_name: str) -> dict:
        """Build psycopg connection parameters dict for the given database name.

        Args:
            db_name: The database name to connect to.

        Returns:
            Dict suitable for ``psycopg.connect(**...)``.

        Raises:
            ValueError: If db_user or db_password is not set.
        """
        if not self.db_user or self.db_password is None:
            raise ValueError(
                "db_user and db_password must be set (via SourceConfig or .env)"
            )
        return {
            "host": self.resolved_db_host,
            "port": self.resolved_db_port,
            "dbname": db_name,
            "user": self.db_user,
            "password": self.db_password,
        }
