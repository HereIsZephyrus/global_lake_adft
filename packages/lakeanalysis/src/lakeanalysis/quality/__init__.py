"""Lake area data quality assessment module."""

from .compute import (
    FlatnessFilterConfig,
    classify_area_anomaly,
    compute_flatness_metrics,
    compute_mean_area,
    compute_median_area,
    is_anomalous,
)
from .frozen import (
    FrozenPlateauSchedule,
    apply_frozen_plateau,
    build_frozen_plateau_schedule,
    defrozen_frame,
    filter_frozen_rows,
    first_frozen_months,
    frozen_run_indices,
    month_index_to_year_month_key,
    year_month_key_to_index,
    year_month_to_key,
)
from .interpolation import (
    CollinearSegment,
    InterpolationConfig,
    InterpolationResult,
    detect_interpolation,
    get_collinear_segments,
)

__all__ = [
    "FlatnessFilterConfig",
    "classify_area_anomaly",
    "compute_flatness_metrics",
    "compute_mean_area",
    "compute_median_area",
    "is_anomalous",
    "FrozenPlateauSchedule",
    "apply_frozen_plateau",
    "build_frozen_plateau_schedule",
    "defrozen_frame",
    "filter_frozen_rows",
    "first_frozen_months",
    "frozen_run_indices",
    "month_index_to_year_month_key",
    "year_month_key_to_index",
    "year_month_to_key",
    "CollinearSegment",
    "InterpolationConfig",
    "InterpolationResult",
    "detect_interpolation",
    "get_collinear_segments",
]
