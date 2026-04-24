"""Unified data source layer for lake analysis.

Provides two backends:
  - postgres: PostgreSQL via psycopg (DBClient, atlas_db, series_db, etc.)
  - parquet:  Local Parquet files via DuckDB (DuckDBClient, create_client)
"""

from __future__ import annotations

from .config import Backend, SourceConfig

__all__ = [
    "Backend",
    "SourceConfig",
    "DuckDBClient",
    "create_client",
    "DBClient",
    "atlas_db",
    "series_db",
    "load_env",
]


def __getattr__(name: str):
    if name in ("DBClient", "atlas_db", "series_db"):
        from .postgres import DBClient, atlas_db, series_db
        return {"DBClient": DBClient, "atlas_db": atlas_db, "series_db": series_db}[name]
    if name in ("DuckDBClient", "create_client"):
        from .parquet import DuckDBClient, create_client
        return {"DuckDBClient": DuckDBClient, "create_client": create_client}[name]
    if name == "load_env":
        from .env import load_env
        return load_env
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
