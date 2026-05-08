"""PostgreSQL backend for lake data access.

This package provides two API levels:

1. **(New) Typed domain repositories**  via ``PostgresBackend`` (preferred):
   >>> from lakesource.postgres.backend import PostgresBackend
   >>> be = PostgresBackend.from_config()
   >>> be.quality.ensure_area_quality_table()
   >>> be.quantile.upsert_quantile_labels(rows)

2. **(Legacy) Module-level functions** via ``__getattr__`` delegation:
   >>> from lakesource.postgres import upsert_area_quality  # deprecated
   >>> upsert_area_quality(conn, rows)

All real implementations live in domain-specific modules:
| Domain       | Module                     |
|-------------|---------------------------|
| area_quality | ``.area_quality_schema``   |
| anomalies    | ``.area_anomalies_schema`` |
| shift_labels | ``.area_shift_labels_schema`` |
| run_status   | ``.quality_run_status_schema`` |
| frozen       | ``.frozen_read``           |
| lake_info    | ``.lake_info_read``        |
| comparison   | ``.comparison_schema``     |
| interpolation| ``.interpolation_detect_schema`` |
| lake_area    | ``.lake_area``             |
| quantile     | ``.lake_quantile``         |
| pwm          | ``.lake_pwm``              |
| eot          | ``.lake_eot``              |
| hawkes       | ``.lake_hawkes``           |
| entropy      | ``.lake_entropy``          |
| hawkes_qc    | ``.hawkes_qc``             |
| compat       | ``.lake`` (deprecated)     |
"""

from .backend import PostgresBackend  # noqa: E402

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

_LEGACY_LAKE_SYMBOLS = {
    "count_area_quality_hylak_ids_in_range",
    "count_pwm_extreme_status_in_range",
    "count_quantile_status_in_range",
    "count_source_hylak_ids_in_range",
    "ensure_area_anomalies_table",
    "ensure_area_quality_table",
    "ensure_area_shift_labels_table",
    "ensure_quality_run_status_table",
    "ensure_area_entropy_cv_table",
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
    "make_quality_run_status_row",
    "RUN_STATUS_DONE",
    "RUN_STATUS_ERROR",
    "upsert_area_anomalies",
    "upsert_area_entropy_cv",
    "upsert_area_quality",
    "upsert_area_shift_labels",
    "upsert_quality_run_status",
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
    "PostgresBackend",
    "ChunkedLakeProcessor",
    "DBClient",
    "atlas_db",
    "series_db",
    "check_extensions",
    *sorted(_LEGACY_LAKE_SYMBOLS),
    *sorted(_COMPARISON_SYMBOLS),
    *sorted(_HAWKES_QC_SYMBOLS),
]

# Symbol sets used by __getattr__ for area_quality related functions
_AREA_QUALITY_SYMBOLS = {
    "count_area_quality_hylak_ids_in_range",
    "ensure_area_anomalies_table",
    "ensure_area_quality_table",
    "ensure_area_shift_labels_table",
    "ensure_quality_run_status_table",
    "fetch_area_quality_hylak_ids",
    "fetch_area_quality_hylak_ids_in_range",
    "fetch_atlas_area_chunk",
    "make_quality_run_status_row",
    "move_area_quality_to_anomalies",
    "RUN_STATUS_DONE",
    "RUN_STATUS_ERROR",
    "upsert_area_anomalies",
    "upsert_area_quality",
    "upsert_area_shift_labels",
    "upsert_quality_run_status",
}


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
    if name in _AREA_QUALITY_SYMBOLS:
        from . import area_quality
        return getattr(area_quality, name)
    if name in _LEGACY_LAKE_SYMBOLS:
        from . import lake
        return getattr(lake, name)
    if name in _HAWKES_QC_SYMBOLS:
        from . import hawkes_qc
        return getattr(hawkes_qc, name)
    if name == "PostgresBackend":
        from .backend import PostgresBackend
        return PostgresBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
