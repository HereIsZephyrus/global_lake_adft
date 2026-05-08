"""PostgreSQL backend for lake data access.

Key entry points:
- ``PostgresBackend``: typed domain repositories (preferred)
- ``DBClient``, ``series_db``, ``atlas_db``: connection management
- ``ChunkedLakeProcessor``: legacy chunked processing

Domain modules:
| Domain       | Module                       |
|-------------|------------------------------|
| area_quality | ``.area_quality_schema``     |
| anomalies    | ``.area_anomalies_schema``   |
| shift_labels | ``.area_shift_labels_schema`` |
| run_status   | ``.quality_run_status_schema`` |
| frozen       | ``.frozen_read``             |
| lake_info    | ``.lake_info_read``          |
| comparison   | ``.comparison_schema``       |
| interpolation| ``.interpolation_detect_schema`` |
| cross_queries| ``.area_cross_queries``      |
| lake_area    | ``.lake_area``               |
| quantile     | ``.lake_quantile``           |
| pwm          | ``.lake_pwm``                |
| eot          | ``.lake_eot``                |
| hawkes       | ``.lake_hawkes``             |
| entropy      | ``.lake_entropy``            |
| hawkes_qc    | ``.hawkes_qc``               |
    | geometry     | ``.lake_geometry``          |
"""

from __future__ import annotations

import importlib

from .backend import PostgresBackend  # noqa: E402

# ------------------------------------------------------------------
# Client symbols
# ------------------------------------------------------------------
_CLIENT_SYMBOLS = {"DBClient", "atlas_db", "series_db"}

# ------------------------------------------------------------------
# Hawkes QC symbols (delegated to hawkes_qc module)
# ------------------------------------------------------------------
_HAWKES_QC_SYMBOLS = {
    "fetch_hawkes_qc_summary_by_quantile",
    "fetch_hawkes_error_message_counts",
    "fetch_hawkes_results",
    "fetch_hawkes_lrt",
    "fetch_hawkes_lrt_summary_by_test",
    "fetch_eot_hawkes_coverage",
    "fetch_hawkes_transition_monthly",
}

# ------------------------------------------------------------------
# Comparison symbols (delegated to comparison package)
# ------------------------------------------------------------------
_COMPARISON_SYMBOLS = {
    "ensure_comparison_tables",
    "upsert_comparison_run_status",
}

# ------------------------------------------------------------------
# Domain-specific symbol mappings
# ------------------------------------------------------------------
_SYMBOL_MAP = {
    # area_quality_schema
    "count_area_quality_hylak_ids_in_range": "area_quality_schema",
    "ensure_area_quality_table": "area_quality_schema",
    "fetch_area_quality_hylak_ids": "area_quality_schema",
    "fetch_area_quality_hylak_ids_in_range": "area_quality_schema",
    "fetch_atlas_area_chunk": "area_quality_schema",
    "upsert_area_quality": "area_quality_schema",
    # area_anomalies_schema
    "ensure_area_anomalies_table": "area_anomalies_schema",
    "move_area_quality_to_anomalies": "area_anomalies_schema",
    "upsert_area_anomalies": "area_anomalies_schema",
    # quality_run_status_schema
    "ensure_quality_run_status_table": "quality_run_status_schema",
    "make_quality_run_status_row": "quality_run_status_schema",
    "RUN_STATUS_DONE": "quality_run_status_schema",
    "RUN_STATUS_ERROR": "quality_run_status_schema",
    "upsert_quality_run_status": "quality_run_status_schema",
    # area_shift_labels_schema
    "ensure_area_shift_labels_table": "area_shift_labels_schema",
    "upsert_area_shift_labels": "area_shift_labels_schema",
    # lake_area
    "fetch_af_nearest_high_topo": "lake_area",
    "fetch_impact_pairs": "lake_area",
    "fetch_lake_area": "lake_area",
    "fetch_lake_area_by_ids": "lake_area",
    "fetch_lake_area_chunk": "lake_area",
    # lake_geometry
    "fetch_lake_geometry_wkt_by_ids": "lake_geometry",
    # frozen_read
    "fetch_frozen_year_months_by_ids": "frozen_read",
    "fetch_frozen_year_months_chunk": "frozen_read",
    # lake_info_read
    "count_source_hylak_ids_in_range": "lake_info_read",
    "fetch_linear_trend_by_ids": "lake_info_read",
    "fetch_max_lake_info_hylak_id": "lake_info_read",
    "fetch_seasonal_amplitude_chunk": "lake_info_read",
    "fetch_source_hylak_ids_in_range": "lake_info_read",
    # comparison_schema
    "fetch_comparison_status_ids_in_range": "comparison_schema",
    # interpolation_detect_schema
    "ensure_interpolation_detect_table": "interpolation_detect_schema",
    "upsert_interpolation_detect": "interpolation_detect_schema",
    # area_cross_queries
    "fetch_anomaly_hylak_ids": "area_cross_queries",
    "fetch_quality_done_hylak_ids_in_range": "area_cross_queries",
    # lake_eot
    "ensure_eot_results_table": "lake_eot",
    "fetch_eot_extremes_by_id": "lake_eot",
    "upsert_eot_extremes": "lake_eot",
    "upsert_eot_results": "lake_eot",
    "upsert_eot_run_status": "lake_eot",
    # lake_quantile
    "count_quantile_status_in_range": "lake_quantile",
    "ensure_quantile_tables": "lake_quantile",
    "fetch_quantile_status_ids_in_range": "lake_quantile",
    "upsert_quantile_abrupt_transitions": "lake_quantile",
    "upsert_quantile_extremes": "lake_quantile",
    "upsert_quantile_labels": "lake_quantile",
    "upsert_quantile_run_status": "lake_quantile",
    # lake_pwm
    "count_pwm_extreme_status_in_range": "lake_pwm",
    "ensure_pwm_extreme_tables": "lake_pwm",
    "fetch_pwm_extreme_status_ids_in_range": "lake_pwm",
    "upsert_pwm_extreme_run_status": "lake_pwm",
    "upsert_pwm_extreme_thresholds": "lake_pwm",
    # lake_hawkes
    "ensure_hawkes_results_table": "lake_hawkes",
    "upsert_hawkes_lrt": "lake_hawkes",
    "upsert_hawkes_results": "lake_hawkes",
    "upsert_hawkes_transition_monthly": "lake_hawkes",
    # lake_entropy
    "ensure_area_entropy_cv_table": "lake_entropy",
    "ensure_entropy_table": "lake_entropy",
    "upsert_area_entropy_cv": "lake_entropy",
    "upsert_entropy": "lake_entropy",
}

__all__ = [
    "PostgresBackend",
    "ChunkedLakeProcessor",
    "DBClient",
    "atlas_db",
    "series_db",
    "check_extensions",
    *sorted(_SYMBOL_MAP.keys()),
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
    if name in _SYMBOL_MAP:
        mod_name = _SYMBOL_MAP[name]
        mod = importlib.import_module(f"lakesource.postgres.{mod_name}")
        return getattr(mod, name)
    if name in _HAWKES_QC_SYMBOLS:
        from . import hawkes_qc
        return getattr(hawkes_qc, name)
    if name == "PostgresBackend":
        from .backend import PostgresBackend
        return PostgresBackend
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
