"""Database connection and extension checks for HydroALTAS (PostGIS + pghydro).

This module provides two data backend options:
1. PostgreSQL backend: DBClient for connecting to PostgreSQL databases
2. DuckDB backend: DuckDBClient for querying Parquet files (no database required)
"""

from .duckdb_client import DuckDBClient, create_client

__all__ = [
    "DuckDBClient",
    "create_client",
]


def __getattr__(name: str):
    if name in (
        "ChunkedLakeProcessor",
        "DBClient",
        "atlas_db",
        "series_db",
        "check_extensions",
        "count_area_quality_hylak_ids_in_range",
        "count_monthly_transition_status_in_range",
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
        "fetch_monthly_transition_status_ids_in_range",
        "ensure_entropy_table",
        "upsert_entropy",
        "ensure_eot_results_table",
        "ensure_hawkes_results_table",
        "ensure_monthly_transition_tables",
        "upsert_eot_results",
        "upsert_eot_extremes",
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
        "upsert_monthly_transition_labels",
        "upsert_monthly_transition_extremes",
        "upsert_monthly_transition_abrupt_transitions",
        "upsert_monthly_transition_run_status",
        "fetch_hawkes_qc_summary_by_quantile",
        "fetch_hawkes_error_message_counts",
        "fetch_hawkes_results",
        "fetch_hawkes_lrt",
        "fetch_hawkes_lrt_summary_by_test",
        "fetch_eot_hawkes_coverage",
        "fetch_hawkes_transition_monthly",
    ):
        try:
            if name == "ChunkedLakeProcessor":
                from .chunked import ChunkedLakeProcessor
                return ChunkedLakeProcessor
            if name in ("DBClient", "atlas_db", "series_db"):
                from .client import DBClient, atlas_db, series_db
                return {"DBClient": DBClient, "atlas_db": atlas_db, "series_db": series_db}[name]
            if name == "check_extensions":
                from .extensions import check_extensions
                return check_extensions
            from . import lake, hawkes_qc
            return {
                "count_area_quality_hylak_ids_in_range": lake.count_area_quality_hylak_ids_in_range,
                "count_monthly_transition_status_in_range": lake.count_monthly_transition_status_in_range,
                "fetch_area_quality_hylak_ids_in_range": lake.fetch_area_quality_hylak_ids_in_range,
                "fetch_lake_area": lake.fetch_lake_area,
                "fetch_lake_area_chunk": lake.fetch_lake_area_chunk,
                "fetch_lake_area_by_ids": lake.fetch_lake_area_by_ids,
                "fetch_lake_geometry_wkt_by_ids": lake.fetch_lake_geometry_wkt_by_ids,
                "fetch_eot_extremes_by_id": lake.fetch_eot_extremes_by_id,
                "fetch_frozen_year_months_by_ids": lake.fetch_frozen_year_months_by_ids,
                "fetch_frozen_year_months_chunk": lake.fetch_frozen_year_months_chunk,
                "fetch_linear_trend_by_ids": lake.fetch_linear_trend_by_ids,
                "fetch_af_nearest_high_topo": lake.fetch_af_nearest_high_topo,
                "fetch_max_area_quality_hylak_id": lake.fetch_max_area_quality_hylak_id,
                "fetch_monthly_transition_status_ids_in_range": lake.fetch_monthly_transition_status_ids_in_range,
                "ensure_entropy_table": lake.ensure_entropy_table,
                "upsert_entropy": lake.upsert_entropy,
                "ensure_eot_results_table": lake.ensure_eot_results_table,
                "ensure_hawkes_results_table": lake.ensure_hawkes_results_table,
                "ensure_monthly_transition_tables": lake.ensure_monthly_transition_tables,
                "upsert_eot_results": lake.upsert_eot_results,
                "upsert_eot_extremes": lake.upsert_eot_extremes,
                "upsert_hawkes_results": lake.upsert_hawkes_results,
                "upsert_hawkes_lrt": lake.upsert_hawkes_lrt,
                "upsert_hawkes_transition_monthly": lake.upsert_hawkes_transition_monthly,
                "fetch_area_quality_hylak_ids": lake.fetch_area_quality_hylak_ids,
                "fetch_atlas_area_chunk": lake.fetch_atlas_area_chunk,
                "fetch_seasonal_amplitude_chunk": lake.fetch_seasonal_amplitude_chunk,
                "move_area_quality_to_anomalies": lake.move_area_quality_to_anomalies,
                "ensure_area_quality_table": lake.ensure_area_quality_table,
                "upsert_area_quality": lake.upsert_area_quality,
                "ensure_area_anomalies_table": lake.ensure_area_anomalies_table,
                "upsert_area_anomalies": lake.upsert_area_anomalies,
                "upsert_monthly_transition_labels": lake.upsert_monthly_transition_labels,
                "upsert_monthly_transition_extremes": lake.upsert_monthly_transition_extremes,
                "upsert_monthly_transition_abrupt_transitions": lake.upsert_monthly_transition_abrupt_transitions,
                "upsert_monthly_transition_run_status": lake.upsert_monthly_transition_run_status,
                "fetch_hawkes_qc_summary_by_quantile": hawkes_qc.fetch_hawkes_qc_summary_by_quantile,
                "fetch_hawkes_error_message_counts": hawkes_qc.fetch_hawkes_error_message_counts,
                "fetch_hawkes_results": hawkes_qc.fetch_hawkes_results,
                "fetch_hawkes_lrt": hawkes_qc.fetch_hawkes_lrt,
                "fetch_hawkes_lrt_summary_by_test": hawkes_qc.fetch_hawkes_lrt_summary_by_test,
                "fetch_eot_hawkes_coverage": hawkes_qc.fetch_eot_hawkes_coverage,
                "fetch_hawkes_transition_monthly": hawkes_qc.fetch_hawkes_transition_monthly,
            }[name]
        except ImportError:
            raise ImportError(
                f"{name} requires PostgreSQL dependencies. "
                f"Install with: pip install 'lakeanalysis[postgres]'"
            ) from None
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
