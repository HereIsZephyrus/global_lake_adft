"""Unified data source layer for lake analysis.

Provides two backends:
  - postgres: PostgreSQL via psycopg (DBClient, atlas_db, series_db, etc.)
  - parquet:  Local Parquet files via DuckDB (DuckDBClient, create_client)
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

__all__ = [
    "Backend",
    "SourceConfig",
    "DuckDBClient",
    "create_client",
    "DBClient",
    "atlas_db",
    "series_db",
]


class Backend(Enum):
    """Supported data backend types."""

    POSTGRES = "postgres"
    PARQUET = "parquet"


class SourceConfig:
    """Configuration for selecting and initializing a data backend.

    Attributes:
        backend: The backend type to use.
        data_dir: Path to Parquet data directory (required when backend is PARQUET).
    """

    def __init__(
        self,
        backend: Backend = Backend.POSTGRES,
        data_dir: str | Path | None = None,
    ) -> None:
        self.backend = backend
        self.data_dir = Path(data_dir) if data_dir else None

    def create(self) -> Any:
        """Instantiate and return the configured backend client.

        Returns:
            A DBClient (postgres) or DuckDBClient (parquet) instance.

        Raises:
            ValueError: If backend is PARQUET and data_dir is not set.
        """
        if self.backend is Backend.PARQUET:
            from .parquet import create_client
            if not self.data_dir:
                raise ValueError("data_dir is required for PARQUET backend")
            return create_client(self.data_dir)
        from .postgres import atlas_db, series_db
        return atlas_db, series_db


def __getattr__(name: str):
    if name in ("DBClient", "atlas_db", "series_db"):
        from .postgres import DBClient, atlas_db, series_db
        return {"DBClient": DBClient, "atlas_db": atlas_db, "series_db": series_db}[name]
    if name in ("DuckDBClient", "create_client"):
        from .parquet import DuckDBClient, create_client
        return {"DuckDBClient": DuckDBClient, "create_client": create_client}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
