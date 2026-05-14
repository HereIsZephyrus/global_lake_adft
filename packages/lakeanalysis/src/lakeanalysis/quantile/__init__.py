"""Monthly anomaly quantile transition workflow."""

from lakeanalysis.extreme.compute import (
    assign_extreme_labels,
    detect_abrupt_transitions,
    extract_extreme_events,
    validate_monthly_series,
)
from .compute import (
    compute_anomaly_thresholds,
    run_monthly_anomaly_transition,
)
from lakesource.quantile.schema import QuantileResult
from .config import QuantileBatchConfig, QuantileServiceConfig
from lakeviz.quantile import (
    plot_anomaly_timeline,
    plot_monthly_timeline,
    plot_transition_count_summary,
    plot_transition_count_summary_precomputed,
    plot_transition_seasonality_summary,
    plot_transition_seasonality_summary_precomputed,
    save_lake_plots,
    save_summary_plots,
)
from .service import run_quantile_service, run_single_lake_service
from .store import (
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
    count_processed_lakes_in_chunk,
    count_source_lakes_in_chunk,
    ensure_quantile_tables,
    fetch_max_hylak_id,
    fetch_processed_hylak_ids_in_chunk,
    fetch_source_hylak_ids_in_chunk,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
    upsert_quantile_abrupt_transitions,
    upsert_quantile_extremes,
    upsert_quantile_labels,
    upsert_quantile_run_status,
)
from .summary import (
    SummaryAccumulator,
    load_summary_cache,
    save_summary_plots,
    write_summary_cache,
)

__all__ = [
    "QuantileServiceConfig",
    "QuantileBatchConfig",
    "QuantileResult",
    "validate_monthly_series",
    "compute_anomaly_thresholds",
    "assign_extreme_labels",
    "extract_extreme_events",
    "detect_abrupt_transitions",
    "run_monthly_anomaly_transition",
    "run_single_lake_service",
    "run_quantile_service",
    "plot_monthly_timeline",
    "plot_anomaly_timeline",
    "plot_transition_count_summary",
    "plot_transition_count_summary_precomputed",
    "plot_transition_seasonality_summary",
    "plot_transition_seasonality_summary_precomputed",
    "save_lake_plots",
    "save_summary_plots",
    "write_summary_cache",
    "load_summary_cache",
    "save_summary_plots",
    "SummaryAccumulator",
    "RUN_STATUS_DONE",
    "RUN_STATUS_ERROR",
    "ensure_quantile_tables",
    "upsert_quantile_labels",
    "upsert_quantile_extremes",
    "upsert_quantile_abrupt_transitions",
    "upsert_quantile_run_status",
    "count_source_lakes_in_chunk",
    "count_processed_lakes_in_chunk",
    "fetch_processed_hylak_ids_in_chunk",
    "fetch_source_hylak_ids_in_chunk",
    "fetch_max_hylak_id",
    "result_to_label_rows",
    "result_to_extreme_rows",
    "result_to_transition_rows",
    "make_run_status_row",
]
