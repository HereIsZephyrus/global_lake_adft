"""Public API for Hawkes modelling utilities."""

from .bridge import build_events_from_eot, build_events_from_pwm
from .mining import (
    build_overall_stats,
    load_events_from_case,
    load_summary,
    safe_series_divide,
    select_transition_lakes,
)
from .fit import (
    HawkesFitter,
    LikelihoodRatioTest,
    fit_full_model,
    fit_restricted_model,
    run_model_comparison,
)
from .model import evaluate_intensity_decomposition
from .pipeline import (
    HawkesCoreResult,
    build_error_summary,
    build_hawkes_result_row,
    build_hawkes_transition_monthly_rows,
    make_hawkes_run_status_row,
    run_hawkes_pipeline,
)
from .plot_adapter import (
    plot_event_timeline,
    plot_intensity_decomposition,
    plot_kernel_matrix,
    plot_lrt_summary,
)
from .types import (
    HawkesEventSeries,
    HawkesFitResult,
    HawkesModelSpec,
    LRTResult,
    ModelComparisonTest,
    TYPE_DRY,
    TYPE_LABELS,
    TYPE_WET,
)

__all__ = [
    "HawkesEventSeries",
    "HawkesModelSpec",
    "HawkesFitResult",
    "LRTResult",
    "ModelComparisonTest",
    "TYPE_DRY",
    "TYPE_WET",
    "TYPE_LABELS",
    "HawkesFitter",
    "LikelihoodRatioTest",
    "fit_full_model",
    "fit_restricted_model",
    "run_model_comparison",
    "evaluate_intensity_decomposition",
    "build_events_from_eot",
    "build_events_from_pwm",
    "HawkesCoreResult",
    "run_hawkes_pipeline",
    "build_error_summary",
    "build_hawkes_result_row",
    "build_hawkes_transition_monthly_rows",
    "make_hawkes_run_status_row",
    "load_summary",
    "safe_series_divide",
    "select_transition_lakes",
    "build_overall_stats",
    "load_events_from_case",
    "plot_event_timeline",
    "plot_intensity_decomposition",
    "plot_kernel_matrix",
    "plot_lrt_summary",
]
