"""PostgreSQL backend for lake data access.

Provides DBClient, ChunkedLakeProcessor, and selected lake/hawkes_qc database
operations. Symbols that require psycopg are lazily loaded via __getattr__ so
that importing this package does not fail when psycopg is not installed.
"""

_CLIENT_SYMBOLS = {"DBClient", "atlas_db", "series_db"}

_HAWKES_QC_SYMBOLS = {
    "fetch_hawkes_qc_summary_by_quantile",
    "fetch_hawkes_error_message_counts",
    "fetch_hawkes_results",
    "fetch_hawkes_lrt",
    "fetch_hawkes_lrt_summary_by_test",
    "fetch_eot_hawkes_coverage",
    "fetch_hawkes_transition_monthly",
}

_COMPARISON_SYMBOLS = {
    "ensure_comparison_tables",
    "upsert_comparison_run_status",
}

_LAKE_SYMBOLS = {
    "count_area_quality_hylak_ids_in_range",
    "count_pwm_extreme_status_in_range",
    "count_quantile_status_in_range",
    "count_source_hylak_ids_in_range",
    "ensure_area_anomalies_table",
    "ensure_area_entropy_cv_table",
    "ensure_area_quality_table",
    "ensure_entropy_table",
    "ensure_eot_results_table",
    "ensure_hawkes_results_table",
    "ensure_interpolation_detect_table",
    "ensure_pwm_extreme_tables",
    "ensure_quantile_tables",
    "fetch_af_nearest_high_topo",
    "fetch_anomaly_hylak_ids",
    "fetch_area_quality_hylak_ids",
    "fetch_area_quality_hylak_ids_in_range",
    "fetch_atlas_area_chunk",
    "fetch_comparison_status_ids_in_range",
    "fetch_eot_extremes_by_id",
    "fetch_frozen_year_months_by_ids",
    "fetch_frozen_year_months_chunk",
    "fetch_impact_pairs",
    "fetch_lake_area",
    "fetch_lake_area_by_ids",
    "fetch_lake_area_chunk",
    "fetch_lake_geometry_wkt_by_ids",
    "fetch_linear_trend_by_ids",
    "fetch_max_lake_info_hylak_id",
    "fetch_pwm_extreme_status_ids_in_range",
    "fetch_quality_done_hylak_ids_in_range",
    "fetch_quantile_status_ids_in_range",
    "fetch_seasonal_amplitude_chunk",
    "fetch_source_hylak_ids_in_range",
    "move_area_quality_to_anomalies",
    "upsert_area_anomalies",
    "upsert_area_entropy_cv",
    "upsert_area_quality",
    "upsert_entropy",
    "upsert_eot_extremes",
    "upsert_eot_results",
    "upsert_eot_run_status",
    "upsert_hawkes_lrt",
    "upsert_hawkes_results",
    "upsert_hawkes_transition_monthly",
    "upsert_interpolation_detect",
    "upsert_pwm_extreme_run_status",
    "upsert_pwm_extreme_thresholds",
    "upsert_quantile_abrupt_transitions",
    "upsert_quantile_extremes",
    "upsert_quantile_labels",
    "upsert_quantile_run_status",
}

__all__ = [
    "ChunkedLakeProcessor",
    "DBClient",
    "atlas_db",
    "series_db",
    "check_extensions",
    *sorted(_LAKE_SYMBOLS),
    *sorted(_COMPARISON_SYMBOLS),
    *sorted(_HAWKES_QC_SYMBOLS),
]


def __getattr__(name: str):
    if name == "ChunkedLakeProcessor":
        from .chunked import ChunkedLakeProcessor

        return ChunkedLakeProcessor
    if name in _CLIENT_SYMBOLS:
        from .client import DBClient, atlas_db, series_db

        return {"DBClient": DBClient, "atlas_db": atlas_db, "series_db": series_db}[name]
    if name == "check_extensions":
        from .extensions import check_extensions

        return check_extensions
    if name in _COMPARISON_SYMBOLS:
        from lakesource import comparison

        return getattr(comparison, name)
    if name in _LAKE_SYMBOLS:
        from . import lake

        return getattr(lake, name)
    if name in _HAWKES_QC_SYMBOLS:
        from . import hawkes_qc

        return getattr(hawkes_qc, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
