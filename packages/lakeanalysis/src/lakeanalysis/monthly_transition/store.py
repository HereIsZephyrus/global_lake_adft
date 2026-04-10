"""DB adapter wrappers and row shapers for monthly transition outputs."""

from __future__ import annotations

from typing import Any

import pandas as pd
import psycopg

from lakeanalysis.dbconnect import (
    count_area_quality_hylak_ids_in_range,
    count_monthly_transition_status_in_range,
    ensure_monthly_transition_tables as ensure_monthly_transition_tables_in_db,
    fetch_max_area_quality_hylak_id,
    fetch_monthly_transition_status_ids_in_range,
    upsert_monthly_transition_abrupt_transitions as upsert_monthly_transition_abrupt_transitions_in_db,
    upsert_monthly_transition_extremes as upsert_monthly_transition_extremes_in_db,
    upsert_monthly_transition_labels as upsert_monthly_transition_labels_in_db,
    upsert_monthly_transition_run_status as upsert_monthly_transition_run_status_in_db,
)

from .compute import MonthlyTransitionResult

RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"
_VALID_RUN_STATUS = {RUN_STATUS_DONE, RUN_STATUS_ERROR}

_FETCH_TRANSITION_COUNTS_SQL = """
SELECT transition_type, COUNT(*)::bigint AS count
FROM monthly_transition_abrupt_transitions
GROUP BY transition_type
"""

_FETCH_TRANSITION_SEASONALITY_SQL = """
SELECT to_month, COUNT(*)::bigint AS count
FROM monthly_transition_abrupt_transitions
GROUP BY to_month
"""

_FETCH_LAKE_TRANSITION_COUNTS_SQL = """
SELECT hylak_id, COUNT(*)::bigint AS transition_count
FROM monthly_transition_abrupt_transitions
GROUP BY hylak_id
"""

_FETCH_LAKE_EXTREME_COUNTS_SQL = """
SELECT hylak_id, COUNT(*)::bigint AS extreme_count
FROM monthly_transition_extremes
GROUP BY hylak_id
"""

_FETCH_RUN_STATUS_COUNTS_SQL = """
SELECT status, COUNT(*)::bigint AS count
FROM monthly_transition_run_status
GROUP BY status
"""

_COUNT_LABEL_ROWS_SQL = "SELECT COUNT(*)::bigint AS count FROM monthly_transition_labels"
_COUNT_EXTREME_ROWS_SQL = "SELECT COUNT(*)::bigint AS count FROM monthly_transition_extremes"
_COUNT_TRANSITION_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM monthly_transition_abrupt_transitions
"""


def ensure_monthly_transition_tables(conn: psycopg.Connection) -> None:
    """Ensure monthly transition DB tables exist."""
    ensure_monthly_transition_tables_in_db(conn)


def upsert_monthly_transition_labels(conn: psycopg.Connection, rows: list[dict]) -> None:
    """Persist month-level label rows."""
    upsert_monthly_transition_labels_in_db(conn, rows)


def upsert_monthly_transition_extremes(conn: psycopg.Connection, rows: list[dict]) -> None:
    """Persist extreme-event rows."""
    upsert_monthly_transition_extremes_in_db(conn, rows)


def upsert_monthly_transition_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict],
) -> None:
    """Persist abrupt-transition rows."""
    upsert_monthly_transition_abrupt_transitions_in_db(conn, rows)


def upsert_monthly_transition_run_status(conn: psycopg.Connection, rows: list[dict]) -> None:
    """Persist run-status rows."""
    upsert_monthly_transition_run_status_in_db(conn, rows)


def count_source_lakes_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> int:
    """Count quality-filtered source lakes in a chunk."""
    return count_area_quality_hylak_ids_in_range(conn, chunk_start, chunk_end)


def count_processed_lakes_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> int:
    """Count run-status rows already written for a chunk."""
    return count_monthly_transition_status_in_range(conn, chunk_start, chunk_end)


def fetch_processed_hylak_ids_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> set[int]:
    """Fetch processed lake ids already present in the run-status table."""
    return fetch_monthly_transition_status_ids_in_range(conn, chunk_start, chunk_end)


def fetch_max_hylak_id(conn: psycopg.Connection) -> int:
    """Return the maximum hylak_id present in area_quality."""
    max_hylak_id = fetch_max_area_quality_hylak_id(conn)
    return 0 if max_hylak_id is None else int(max_hylak_id)


def result_to_label_rows(result: MonthlyTransitionResult) -> list[dict]:
    """Convert one-lake label output to DB row dicts."""
    columns = [
        "hylak_id",
        "year",
        "month",
        "water_area",
        "monthly_climatology",
        "anomaly",
        "q_low",
        "q_high",
        "extreme_label",
    ]
    return result.labels_df.loc[:, columns].to_dict("records")


def result_to_extreme_rows(result: MonthlyTransitionResult) -> list[dict]:
    """Convert one-lake extreme output to DB row dicts."""
    if result.extremes_df.empty:
        return []
    return result.extremes_df.to_dict("records")


def result_to_transition_rows(result: MonthlyTransitionResult) -> list[dict]:
    """Convert one-lake abrupt-transition output to DB row dicts."""
    if result.transitions_df.empty:
        return []
    return result.transitions_df.to_dict("records")


def make_run_status_row(
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
    status: str,
    error_message: str | None = None,
) -> dict:
    """Create a validated run-status row."""
    if status not in _VALID_RUN_STATUS:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": int(hylak_id),
        "chunk_start": int(chunk_start),
        "chunk_end": int(chunk_end),
        "status": status,
        "error_message": None if error_message is None else str(error_message)[:500],
    }


def fetch_summary_cache_sources(conn: psycopg.Connection) -> dict[str, Any]:
    """Fetch aggregate frames for local summary cache refresh."""

    def _query_frame(sql_text: str, columns: list[str]) -> pd.DataFrame:
        with conn.cursor() as cur:
            cur.execute(sql_text)
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)

    transition_counts = _query_frame(
        _FETCH_TRANSITION_COUNTS_SQL,
        ["transition_type", "count"],
    )
    transition_seasonality = _query_frame(
        _FETCH_TRANSITION_SEASONALITY_SQL,
        ["to_month", "count"],
    )
    lake_transition_counts = _query_frame(
        _FETCH_LAKE_TRANSITION_COUNTS_SQL,
        ["hylak_id", "transition_count"],
    )
    lake_extreme_counts = _query_frame(
        _FETCH_LAKE_EXTREME_COUNTS_SQL,
        ["hylak_id", "extreme_count"],
    )
    status_counts = _query_frame(_FETCH_RUN_STATUS_COUNTS_SQL, ["status", "count"])

    with conn.cursor() as cur:
        cur.execute(_COUNT_LABEL_ROWS_SQL)
        labels_count = int(cur.fetchone()[0])
        cur.execute(_COUNT_EXTREME_ROWS_SQL)
        extremes_count = int(cur.fetchone()[0])
        cur.execute(_COUNT_TRANSITION_ROWS_SQL)
        transitions_count = int(cur.fetchone()[0])

    status_map = {
        str(row["status"]): int(row["count"])
        for _, row in status_counts.iterrows()
    }
    metadata = {
        "labels_rows": labels_count,
        "extremes_rows": extremes_count,
        "transitions_rows": transitions_count,
        "status_done": status_map.get(RUN_STATUS_DONE, 0),
        "status_error": status_map.get(RUN_STATUS_ERROR, 0),
    }
    return {
        "transition_counts": transition_counts,
        "transition_seasonality": transition_seasonality,
        "lake_transition_counts": lake_transition_counts,
        "lake_extreme_counts": lake_extreme_counts,
        "run_metadata": metadata,
    }
