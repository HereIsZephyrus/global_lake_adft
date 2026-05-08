"""PostgresBackend: typed domain repository assembler.

Replaces PostgresLakeProvider's if/elif table_name dispatch with
explicitly-typed domain repositories.
"""

from __future__ import annotations

from dataclasses import dataclass

from lakesource.config import SourceConfig

from .repositories.area_read import PostgresLakeAreaReadRepository
from .repositories.metadata_read import PostgresMetadataReadRepository
from .repositories.quality_read import PostgresQualityReadRepository
from .repositories.quality_write import PostgresQualityWriteRepository
from .repositories.anomalies_write import PostgresAnomaliesWriteRepository
from .repositories.shift_labels_write import PostgresShiftLabelsRepository
from .repositories.algorithm_writes import (
    PostgresQuantileWriteRepository,
    PostgresPwmWriteRepository,
    PostgresEotWriteRepository,
    PostgresHawkesWriteRepository,
)
from .repositories.comparison_write import (
    PostgresComparisonWriteRepository,
    PostgresInterpolationDetectWriteRepository,
    PostgresEntropyWriteRepository,
)
from .repositories.geometry_read import PostgresGeometryReadRepository


def _series_conn_factory():
    from lakesource.postgres import series_db
    return series_db.connection_context()


@dataclass
class PostgresBackend:
    """Typed domain repositories for PostgreSQL.

    Usage:
        backend = PostgresBackend.from_config()
        backend.quality.ensure_area_quality_table()
        backend.quantile.upsert_quantile_labels(rows)
    """

    area: PostgresLakeAreaReadRepository
    metadata: PostgresMetadataReadRepository
    quality_read: PostgresQualityReadRepository
    quality: PostgresQualityWriteRepository
    anomalies: PostgresAnomaliesWriteRepository
    shift_labels: PostgresShiftLabelsRepository
    quantile: PostgresQuantileWriteRepository
    pwm: PostgresPwmWriteRepository
    eot: PostgresEotWriteRepository
    hawkes: PostgresHawkesWriteRepository
    comparison: PostgresComparisonWriteRepository
    interpolation: PostgresInterpolationDetectWriteRepository
    entropy: PostgresEntropyWriteRepository
    geometry: PostgresGeometryReadRepository

    @classmethod
    def from_config(cls, config: SourceConfig | None = None) -> "PostgresBackend":
        cfg = config or SourceConfig()
        tc = cfg.t
        factory = _series_conn_factory
        return cls(
            area=PostgresLakeAreaReadRepository(factory, table_config=tc),
            metadata=PostgresMetadataReadRepository(factory, table_config=tc),
            quality_read=PostgresQualityReadRepository(factory, table_config=tc),
            quality=PostgresQualityWriteRepository(factory, table_config=tc),
            anomalies=PostgresAnomaliesWriteRepository(factory, table_config=tc),
            shift_labels=PostgresShiftLabelsRepository(factory, table_config=tc),
            quantile=PostgresQuantileWriteRepository(factory, table_config=tc),
            pwm=PostgresPwmWriteRepository(factory, table_config=tc),
            eot=PostgresEotWriteRepository(factory, table_config=tc),
            hawkes=PostgresHawkesWriteRepository(factory, table_config=tc),
            comparison=PostgresComparisonWriteRepository(factory, table_config=tc),
            interpolation=PostgresInterpolationDetectWriteRepository(factory, table_config=tc),
            entropy=PostgresEntropyWriteRepository(factory, table_config=tc),
            geometry=PostgresGeometryReadRepository(factory, table_config=tc),
        )
