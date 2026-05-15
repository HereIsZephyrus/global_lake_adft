"""LakeProvider: unified data access strategy.

ParquetLakeProvider implements the LakeProvider ABC and handles all read
operations via DuckDB.  PostgresLakeProvider is a standalone class for
PostgreSQL write/persistence and spatial queries — it does NOT implement
the LakeProvider ABC.
"""

from .base import LakeProvider
from .factory import create_provider
from .postgres_provider import PostgresLakeProvider
from .parquet_provider import ParquetLakeProvider

__all__ = [
    "LakeProvider",
    "ParquetLakeProvider",
    "PostgresLakeProvider",
    "create_provider",
]
