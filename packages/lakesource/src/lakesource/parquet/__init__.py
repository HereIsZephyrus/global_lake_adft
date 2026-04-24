"""Parquet file backend for lake data access via DuckDB."""

from .client import DuckDBClient, create_client

__all__ = [
    "DuckDBClient",
    "create_client",
]
