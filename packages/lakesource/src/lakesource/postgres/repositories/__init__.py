"""Postgres repository implementations."""

from .area_read import PostgresLakeAreaReadRepository
from .metadata_read import PostgresMetadataReadRepository
from .quality_read import PostgresQualityReadRepository
from .quality_write import PostgresQualityWriteRepository
from .anomalies_write import PostgresAnomaliesWriteRepository
from .shift_labels_write import PostgresShiftLabelsRepository
from .algorithm_writes import (
    PostgresQuantileWriteRepository,
    PostgresPwmWriteRepository,
    PostgresEotWriteRepository,
    PostgresHawkesWriteRepository,
)
from .comparison_write import (
    PostgresComparisonWriteRepository,
    PostgresInterpolationDetectWriteRepository,
    PostgresEntropyWriteRepository,
)
from .geometry_read import PostgresGeometryReadRepository

__all__ = [
    "PostgresLakeAreaReadRepository",
    "PostgresMetadataReadRepository",
    "PostgresQualityReadRepository",
    "PostgresQualityWriteRepository",
    "PostgresAnomaliesWriteRepository",
    "PostgresShiftLabelsRepository",
    "PostgresQuantileWriteRepository",
    "PostgresPwmWriteRepository",
    "PostgresEotWriteRepository",
    "PostgresHawkesWriteRepository",
    "PostgresComparisonWriteRepository",
    "PostgresInterpolationDetectWriteRepository",
    "PostgresEntropyWriteRepository",
    "PostgresGeometryReadRepository",
]
