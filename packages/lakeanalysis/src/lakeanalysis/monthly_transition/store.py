"""DB adapter wrappers and row shapers for monthly transition outputs.

Re-exported from lakesource.monthly_transition.store to avoid duplication.
"""

from __future__ import annotations

from lakesource.monthly_transition.store import (  # noqa: F401
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
    count_processed_lakes_in_chunk,
    count_source_lakes_in_chunk,
    ensure_monthly_transition_tables,
    fetch_max_hylak_id,
    fetch_processed_hylak_ids_in_chunk,
    fetch_source_hylak_ids_in_chunk,
    fetch_summary_cache_sources,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
    upsert_monthly_transition_abrupt_transitions,
    upsert_monthly_transition_extremes,
    upsert_monthly_transition_labels,
    upsert_monthly_transition_run_status,
)
