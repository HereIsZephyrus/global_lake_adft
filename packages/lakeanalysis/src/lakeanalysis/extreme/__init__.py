"""Shared extreme-event utilities and models."""

from .compute import (
    assign_extreme_labels,
    detect_abrupt_transitions,
    extract_extreme_events,
    validate_monthly_series,
)
from .evt import (
    DEFAULT_RETURN_PERIODS,
    EVT_STRENGTH_COLS,
    EVT_SUMMARY_COLS,
    ROUTE_A,
    ROUTE_B,
    build_empty_tail_summary_rows,
    build_fitted_tail_summary_rows,
    compute_return_level,
    fit_gpd_exceedances,
)
from .models import ExtremeResult, PWMDiagnostics, QuantileDiagnostics
from .service import create_decomposition_method, run_single_lake_service

__all__ = [
    "ExtremeResult",
    "QuantileDiagnostics",
    "PWMDiagnostics",
    "validate_monthly_series",
    "assign_extreme_labels",
    "extract_extreme_events",
    "detect_abrupt_transitions",
    "create_decomposition_method",
    "run_single_lake_service",
    "DEFAULT_RETURN_PERIODS",
    "ROUTE_A",
    "ROUTE_B",
    "EVT_SUMMARY_COLS",
    "EVT_STRENGTH_COLS",
    "fit_gpd_exceedances",
    "compute_return_level",
    "build_empty_tail_summary_rows",
    "build_fitted_tail_summary_rows",
]
