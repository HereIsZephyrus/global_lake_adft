"""Re-export shim kept for backward compatibility.

All implementations have been moved to domain-specific modules:
- lakesource.postgres.area_quality_schema
- lakesource.postgres.area_anomalies_schema
- lakesource.postgres.quality_run_status_schema
- lakesource.postgres.area_shift_labels_schema
"""

from __future__ import annotations

from lakesource.postgres.area_quality_schema import (
    fetch_atlas_area_chunk,
    ensure_area_quality_table,
    upsert_area_quality,
    fetch_area_quality_hylak_ids,
    fetch_area_quality_hylak_ids_in_range,
    count_area_quality_hylak_ids_in_range,
    _fetch_atlas_area_chunk_sql,
    _ensure_area_quality_table_sql,
    _upsert_area_quality_sql,
    _fetch_area_quality_hylak_ids_sql,
    _fetch_area_quality_ids_in_range_sql,
    _count_area_quality_in_range_sql,
)
from lakesource.postgres.area_anomalies_schema import (
    ensure_area_anomalies_table,
    upsert_area_anomalies,
    move_area_quality_to_anomalies,
    _ensure_area_anomalies_table_sql,
    _ensure_area_processed_view_sql,
    _upsert_area_anomalies_sql,
    _fetch_anomaly_hylak_ids_sql,
    _move_area_quality_to_anomalies_sql,
    _delete_area_quality_by_ids_sql,
)
from lakesource.postgres.quality_run_status_schema import (
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
    ensure_quality_run_status_table,
    upsert_quality_run_status,
    make_quality_run_status_row,
    _ensure_quality_run_status_table_sql,
    _upsert_quality_run_status_sql,
)
from lakesource.postgres.area_shift_labels_schema import (
    _nan_to_none,
    ensure_area_shift_labels_table,
    upsert_area_shift_labels,
    truncate_area_shift_labels,
    _ensure_area_shift_labels_table_sql,
    _upsert_area_shift_labels_sql,
    _truncate_area_shift_labels_sql,
)
