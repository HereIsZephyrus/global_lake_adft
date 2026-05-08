"""Re-export shim kept for backward compatibility.

All implementations have been moved to domain-specific modules:
- lakesource.postgres.frozen_read
- lakesource.postgres.lake_info_read
- lakesource.postgres.comparison_schema
- lakesource.postgres.interpolation_detect_schema
- lakesource.postgres.area_cross_queries
"""

from __future__ import annotations

from lakesource.postgres.frozen_read import (
    fetch_frozen_year_months_by_ids,
    fetch_frozen_year_months_chunk,
    _fetch_frozen_year_months_by_ids_sql,
    _fetch_frozen_year_months_chunk_sql,
)
from lakesource.postgres.lake_info_read import (
    fetch_seasonal_amplitude_chunk,
    fetch_linear_trend_by_ids,
    fetch_max_lake_info_hylak_id,
    count_source_hylak_ids_in_range,
    fetch_source_hylak_ids_in_range,
    _fetch_seasonal_amplitude_chunk_sql,
    _fetch_linear_trend_by_ids_sql,
    _fetch_max_lake_info_hylak_id_sql,
    _count_source_hylak_ids_in_range_sql,
    _fetch_source_hylak_ids_in_range_sql,
)
from lakesource.postgres.comparison_schema import (
    ensure_comparison_tables,
    upsert_comparison_run_status,
    fetch_comparison_status_ids_in_range,
    _ensure_comparison_run_status_table_sql,
    _upsert_comparison_run_status_sql,
    _fetch_comparison_status_ids_in_range_sql,
)
from lakesource.postgres.interpolation_detect_schema import (
    ensure_interpolation_detect_table,
    upsert_interpolation_detect,
    _ensure_interpolation_detect_table_sql,
    _upsert_interpolation_detect_sql,
)
from lakesource.postgres.area_cross_queries import (
    fetch_anomaly_hylak_ids,
    fetch_quality_done_hylak_ids_in_range,
    _fetch_anomaly_hylak_ids_sql,
    _fetch_quality_done_hylak_ids_in_range_sql,
    _fetch_area_quality_ids_in_range_sql,
    _count_area_quality_in_range_sql,
)
