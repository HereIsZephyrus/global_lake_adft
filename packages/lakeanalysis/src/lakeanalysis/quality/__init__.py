"""Lake area data quality assessment module."""

from .compute import (
    FlatnessFilterConfig,
    classify_area_anomaly,
    compute_flatness_metrics,
    compute_mean_area,
    compute_median_area,
    is_anomalous,
)

__all__ = [
    "FlatnessFilterConfig",
    "classify_area_anomaly",
    "compute_flatness_metrics",
    "compute_mean_area",
    "compute_median_area",
    "is_anomalous",
]
