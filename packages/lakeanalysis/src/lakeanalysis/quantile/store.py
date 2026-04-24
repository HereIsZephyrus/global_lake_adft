"""DB adapter wrappers and row shapers for monthly transition outputs.

Re-exported from lakesource.quantile.store to avoid duplication.
"""

from __future__ import annotations

from lakesource.quantile.store import (  # noqa: F401
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
    count_processed_lakes_in_chunk,
    count_source_lakes_in_chunk,
    ensure_quantile_tables,
    fetch_max_hylak_id,
    fetch_processed_hylak_ids_in_chunk,
    fetch_source_hylak_ids_in_chunk,
    fetch_summary_cache_sources,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
    upsert_quantile_abrupt_transitions,
    upsert_quantile_extremes,
    upsert_quantile_labels,
    upsert_quantile_run_status,
)
