"""LakeProvider: unified data access strategy for PostgreSQL and Parquet backends.

LakeProvider combines read and write capabilities behind a single ABC.
Consumers (lakeanalysis batch, lakeviz) receive a LakeProvider instance
from the factory and call methods without knowing the backend.

PostGIS-dependent aggregation queries (grid maps) use pre-computed
lat/lon columns when running against Parquet/DuckDB.
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
