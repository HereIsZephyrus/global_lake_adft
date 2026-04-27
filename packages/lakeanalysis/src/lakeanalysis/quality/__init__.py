"""Lake area data quality assessment module."""

from .comparison import (
    AgreementConfig,
    classify_agreement,
    compute_area_ratio,
    compute_log2_ratio,
    compute_relative_diff,
    enrich_comparison_df,
    summarize_comparison,
)
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
    "AgreementConfig",
    "FlatnessFilterConfig",
    "classify_agreement",
    "classify_area_anomaly",
    "compute_area_ratio",
    "compute_flatness_metrics",
    "compute_log2_ratio",
    "compute_mean_area",
    "compute_median_area",
    "compute_relative_diff",
    "enrich_comparison_df",
    "is_anomalous",
    "summarize_comparison",
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
