"""Factory: create LakeProvider based on SourceConfig.backend."""

from __future__ import annotations

from lakesource.config import Backend, SourceConfig

from .base import LakeProvider
from .parquet_provider import ParquetLakeProvider
from .postgres_provider import PostgresLakeProvider


def create_provider(config: SourceConfig | None = None) -> LakeProvider:
    config = config or SourceConfig()
    if config.backend == Backend.POSTGRES:
        return PostgresLakeProvider(config)
    if config.backend == Backend.PARQUET:
        return ParquetLakeProvider(config)
    raise ValueError(f"Unsupported backend: {config.backend!r}")
