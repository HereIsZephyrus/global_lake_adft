"""DB adapter wrappers and row shapers for monthly transition outputs."""

from __future__ import annotations

from typing import Any

import pandas as pd
import psycopg

from lakesource.postgres.lake import (
    count_area_quality_hylak_ids_in_range,
    count_monthly_transition_status_in_range,
    fetch_area_quality_hylak_ids_in_range,
    fetch_max_area_quality_hylak_id,
    fetch_monthly_transition_status_ids_in_range,
    upsert_monthly_transition_abrupt_transitions as upsert_monthly_transition_abrupt_transitions_in_db,
    upsert_monthly_transition_extremes as upsert_monthly_transition_extremes_in_db,
    upsert_monthly_transition_labels as upsert_monthly_transition_labels_in_db,
    upsert_monthly_transition_run_status as upsert_monthly_transition_run_status_in_db,
)
from lakesource.postgres.lake import ensure_monthly_transition_tables as ensure_monthly_transition_tables_in_db

from .schema import MonthlyTransitionResult, RUN_STATUS_DONE, RUN_STATUS_ERROR

_VALID_RUN_STATUS = {RUN_STATUS_DONE, RUN_STATUS_ERROR}

_FETCH_TRANSITION_COUNTS_SQL = """
SELECT transition_type, COUNT(*)::bigint AS count
FROM monthly_transition_abrupt_transitions
WHERE workflow_version = %(workflow_version)s
GROUP BY transition_type
"""

_FETCH_TRANSITION_SEASONALITY_SQL = """
SELECT to_month, COUNT(*)::bigint AS count
FROM monthly_transition_abrupt_transitions
WHERE workflow_version = %(workflow_version)s
GROUP BY to_month
"""

_FETCH_LAKE_TRANSITION_COUNTS_SQL = """
SELECT hylak_id, COUNT(*)::bigint AS transition_count
FROM monthly_transition_abrupt_transitions
WHERE workflow_version = %(workflow_version)s
GROUP BY hylak_id
"""

_FETCH_LAKE_EXTREME_COUNTS_SQL = """
SELECT hylak_id, COUNT(*)::bigint AS extreme_count
FROM monthly_transition_extremes
WHERE workflow_version = %(workflow_version)s
GROUP BY hylak_id
"""

_FETCH_RUN_STATUS_COUNTS_SQL = """
SELECT status, COUNT(*)::bigint AS count
FROM monthly_transition_run_status
WHERE workflow_version = %(workflow_version)s
GROUP BY status
"""

_COUNT_LABEL_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM monthly_transition_labels
WHERE workflow_version = %(workflow_version)s
"""
_COUNT_EXTREME_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM monthly_transition_extremes
WHERE workflow_version = %(workflow_version)s
"""
_COUNT_TRANSITION_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM monthly_transition_abrupt_transitions
WHERE workflow_version = %(workflow_version)s
"""


def ensure_monthly_transition_tables(conn: psycopg.Connection) -> None:
    ensure_monthly_transition_tables_in_db(conn)


def upsert_monthly_transition_labels(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_monthly_transition_labels_in_db(conn, rows, commit=commit)


def upsert_monthly_transition_extremes(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_monthly_transition_extremes_in_db(conn, rows, commit=commit)


def upsert_monthly_transition_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_monthly_transition_abrupt_transitions_in_db(
        conn,
        rows,
        commit=commit,
    )


def upsert_monthly_transition_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_monthly_transition_run_status_in_db(conn, rows, commit=commit)


def count_source_lakes_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> int:
    return count_area_quality_hylak_ids_in_range(conn, chunk_start, chunk_end)


def count_processed_lakes_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
) -> int:
    return count_monthly_transition_status_in_range(
        conn,
        chunk_start,
        chunk_end,
        workflow_version=workflow_version,
    )


def fetch_source_hylak_ids_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> set[int]:
    return fetch_area_quality_hylak_ids_in_range(conn, chunk_start, chunk_end)


def fetch_processed_hylak_ids_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
) -> set[int]:
    return fetch_monthly_transition_status_ids_in_range(
        conn,
        chunk_start,
        chunk_end,
        workflow_version=workflow_version,
    )


def fetch_max_hylak_id(conn: psycopg.Connection) -> int:
    max_hylak_id = fetch_max_area_quality_hylak_id(conn)
    return 0 if max_hylak_id is None else int(max_hylak_id)


def result_to_label_rows(
    result: MonthlyTransitionResult,
    *,
    workflow_version: str,
) -> list[dict]:
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
    return _attach_workflow_version(
        result.labels_df.loc[:, columns].to_dict("records"),
        workflow_version=workflow_version,
    )


def result_to_extreme_rows(
    result: MonthlyTransitionResult,
    *,
    workflow_version: str,
) -> list[dict]:
    if result.extremes_df.empty:
        return []
    return _attach_workflow_version(
        result.extremes_df.to_dict("records"),
        workflow_version=workflow_version,
    )


def result_to_transition_rows(
    result: MonthlyTransitionResult,
    *,
    workflow_version: str,
) -> list[dict]:
    if result.transitions_df.empty:
        return []
    return _attach_workflow_version(
        result.transitions_df.to_dict("records"),
        workflow_version=workflow_version,
    )


def _attach_workflow_version(
    rows: list[dict],
    *,
    workflow_version: str,
) -> list[dict]:
    version = workflow_version.strip()
    if not version:
        raise ValueError("workflow_version must not be empty")
    return [{**row, "workflow_version": version} for row in rows]


def make_run_status_row(
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
    workflow_version: str,
    status: str,
    error_message: str | None = None,
) -> dict:
    if status not in _VALID_RUN_STATUS:
        raise ValueError(f"Invalid run status: {status!r}")
    if not workflow_version.strip():
        raise ValueError("workflow_version must not be empty")
    return {
        "hylak_id": int(hylak_id),
        "chunk_start": int(chunk_start),
        "chunk_end": int(chunk_end),
        "workflow_version": workflow_version.strip(),
        "status": status,
        "error_message": None if error_message is None else str(error_message)[:500],
    }


def fetch_summary_cache_sources(
    conn: psycopg.Connection,
    *,
    workflow_version: str,
) -> dict[str, Any]:
    query_params = {"workflow_version": workflow_version}

    def _query_frame(sql_text: str, columns: list[str]) -> pd.DataFrame:
        with conn.cursor() as cur:
            cur.execute(sql_text, query_params)
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
        cur.execute(_COUNT_LABEL_ROWS_SQL, query_params)
        labels_count = int(cur.fetchone()[0])
        cur.execute(_COUNT_EXTREME_ROWS_SQL, query_params)
        extremes_count = int(cur.fetchone()[0])
        cur.execute(_COUNT_TRANSITION_ROWS_SQL, query_params)
        transitions_count = int(cur.fetchone()[0])

    status_map = {
        str(row["status"]): int(row["count"]) for _, row in status_counts.iterrows()
    }
    metadata = {
        "labels_rows": labels_count,
        "extremes_rows": extremes_count,
        "transitions_rows": transitions_count,
        "done_count": status_map.get(RUN_STATUS_DONE, 0),
        "error_count": status_map.get(RUN_STATUS_ERROR, 0),
        "workflow_version": workflow_version,
    }
    return {
        "transition_counts": transition_counts,
        "transition_seasonality": transition_seasonality,
        "lake_transition_counts": lake_transition_counts,
        "lake_extreme_counts": lake_extreme_counts,
        "run_metadata": metadata,
    }
