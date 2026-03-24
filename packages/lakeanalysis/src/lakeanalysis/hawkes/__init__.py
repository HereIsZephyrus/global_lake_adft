"""Public API for Hawkes modelling utilities."""

from .bridge import build_events_from_eot
from .fit import (
    HawkesFitter,
    LikelihoodRatioTest,
    fit_full_model,
    fit_restricted_model,
    run_model_comparison,
)
from .model import evaluate_intensity_decomposition
from .plot import (
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
    "plot_event_timeline",
    "plot_intensity_decomposition",
    "plot_kernel_matrix",
    "plot_lrt_summary",
]

