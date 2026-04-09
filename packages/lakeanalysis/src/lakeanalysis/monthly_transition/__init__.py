"""Monthly anomaly quantile transition workflow."""

from .compute import (
    MonthlyTransitionResult,
    assign_extreme_labels,
    compute_anomaly_thresholds,
    compute_monthly_anomalies,
    compute_monthly_climatology,
    detect_abrupt_transitions,
    extract_extreme_events,
    run_monthly_anomaly_transition,
    validate_monthly_series,
)
from .plot import (
    plot_anomaly_timeline,
    plot_monthly_timeline,
    plot_transition_count_summary,
    plot_transition_seasonality_summary,
    save_lake_plots,
    save_summary_plots,
)

__all__ = [
    "MonthlyTransitionResult",
    "validate_monthly_series",
    "compute_monthly_climatology",
    "compute_monthly_anomalies",
    "compute_anomaly_thresholds",
    "assign_extreme_labels",
    "extract_extreme_events",
    "detect_abrupt_transitions",
    "run_monthly_anomaly_transition",
    "plot_monthly_timeline",
    "plot_anomaly_timeline",
    "plot_transition_count_summary",
    "plot_transition_seasonality_summary",
    "save_lake_plots",
    "save_summary_plots",
]
