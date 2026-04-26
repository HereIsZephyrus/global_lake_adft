"""PostgreSQL backend for lake data access.

Provides DBClient, ChunkedLakeProcessor, and all lake/hawkes_qc database
operations. Symbols that require psycopg are lazily loaded via __getattr__
so that importing this package does not fail when psycopg is not installed.
"""

__all__ = [
    "ChunkedLakeProcessor",
    "DBClient",
    "atlas_db",
    "check_extensions",
    "count_area_quality_hylak_ids_in_range",
    "count_quantile_status_in_range",
    "ensure_area_anomalies_table",
    "ensure_area_quality_table",
    "ensure_entropy_table",
    "ensure_eot_results_table",
    "ensure_hawkes_results_table",
    "ensure_quantile_tables",
    "fetch_area_quality_hylak_ids",
    "fetch_area_quality_hylak_ids_in_range",
    "fetch_atlas_area_chunk",
    "fetch_af_nearest_high_topo",
    "fetch_eot_extremes_by_id",
    "fetch_eot_hawkes_coverage",
    "fetch_frozen_year_months_by_ids",
    "fetch_frozen_year_months_chunk",
    "fetch_hawkes_error_message_counts",
    "fetch_hawkes_lrt",
    "fetch_hawkes_lrt_summary_by_test",
    "fetch_hawkes_qc_summary_by_quantile",
    "fetch_hawkes_results",
    "fetch_hawkes_transition_monthly",
    "fetch_lake_area",
    "fetch_lake_area_by_ids",
    "fetch_lake_area_chunk",
    "fetch_lake_geometry_wkt_by_ids",
    "fetch_linear_trend_by_ids",
    "fetch_max_area_quality_hylak_id",
    "fetch_quantile_status_ids_in_range",
    "fetch_seasonal_amplitude_chunk",
    "move_area_quality_to_anomalies",
    "series_db",
    "upsert_area_anomalies",
    "upsert_area_quality",
    "upsert_entropy",
    "upsert_eot_extremes",
    "upsert_eot_results",
    "upsert_eot_run_status",
    "upsert_hawkes_lrt",
    "upsert_hawkes_results",
    "upsert_hawkes_transition_monthly",
    "upsert_quantile_abrupt_transitions",
    "upsert_quantile_extremes",
    "upsert_quantile_labels",
    "upsert_quantile_run_status",
    "ensure_comparison_tables",
    "upsert_comparison_run_status",
    "fetch_comparison_status_ids_in_range",
]


def __getattr__(name: str):
    if name == "ChunkedLakeProcessor":
        from .chunked import ChunkedLakeProcessor
        return ChunkedLakeProcessor
    if name in ("DBClient", "atlas_db", "series_db"):
        from .client import DBClient, atlas_db, series_db
        return {"DBClient": DBClient, "atlas_db": atlas_db, "series_db": series_db}[name]
    if name == "check_extensions":
        from .extensions import check_extensions
        return check_extensions
    _LAKE_SYMBOLS = {
        "count_area_quality_hylak_ids_in_range",
"count_pwm_extreme_status_in_range",
        "count_quantile_status_in_range",
        "fetch_area_quality_hylak_ids_in_range",
        "fetch_lake_area",
        "fetch_lake_area_chunk",
        "fetch_lake_area_by_ids",
        "fetch_lake_geometry_wkt_by_ids",
        "fetch_eot_extremes_by_id",
        "fetch_frozen_year_months_by_ids",
        "fetch_frozen_year_months_chunk",
        "fetch_linear_trend_by_ids",
        "fetch_af_nearest_high_topo",
        "fetch_max_area_quality_hylak_id",
        "fetch_quantile_status_ids_in_range",
        "fetch_pwm_extreme_status_ids_in_range",
        "ensure_entropy_table",
        "upsert_entropy",
        "ensure_eot_results_table",
        "ensure_hawkes_results_table",
        "ensure_pwm_extreme_tables",
        "ensure_quantile_tables",
        "upsert_eot_results",
        "upsert_eot_extremes",
        "upsert_eot_run_status",
        "upsert_hawkes_results",
        "upsert_hawkes_lrt",
        "upsert_hawkes_transition_monthly",
        "fetch_area_quality_hylak_ids",
        "fetch_atlas_area_chunk",
        "fetch_seasonal_amplitude_chunk",
        "move_area_quality_to_anomalies",
        "ensure_area_quality_table",
        "upsert_area_quality",
        "ensure_area_anomalies_table",
        "upsert_area_anomalies",
        "upsert_pwm_extreme_thresholds",
        "upsert_pwm_extreme_run_status",
        "upsert_quantile_labels",
        "upsert_quantile_extremes",
    "count_pwm_extreme_status_in_range",
    "ensure_pwm_extreme_tables",
    "fetch_pwm_extreme_status_ids_in_range",
    "upsert_pwm_extreme_run_status",
    "upsert_pwm_extreme_thresholds",
    "upsert_eot_run_status",
    "upsert_quantile_abrupt_transitions",
        "upsert_quantile_run_status",
        "ensure_comparison_tables",
        "upsert_comparison_run_status",
        "fetch_comparison_status_ids_in_range",
    }
    _HAWKES_QC_SYMBOLS = {
        "fetch_hawkes_qc_summary_by_quantile",
        "fetch_hawkes_error_message_counts",
        "fetch_hawkes_results",
        "fetch_hawkes_lrt",
        "fetch_hawkes_lrt_summary_by_test",
        "fetch_eot_hawkes_coverage",
        "fetch_hawkes_transition_monthly",
    }
    if name in _LAKE_SYMBOLS:
        from . import lake
        return getattr(lake, name)
    if name in _HAWKES_QC_SYMBOLS:
        from . import hawkes_qc
        return getattr(hawkes_qc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
