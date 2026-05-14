"""DB adapter wrappers and row shapers for monthly transition outputs."""

from __future__ import annotations

from typing import Any

import pandas as pd
import psycopg

from lakesource.postgres.lake_quantile import (
    count_quantile_status_in_range,
    ensure_quantile_tables as ensure_quantile_tables_in_db,
    fetch_quantile_status_ids_in_range,
    upsert_quantile_abrupt_transitions as upsert_quantile_abrupt_transitions_in_db,
    upsert_quantile_extremes as upsert_quantile_extremes_in_db,
    upsert_quantile_labels as upsert_quantile_labels_in_db,
    upsert_quantile_run_status as upsert_quantile_run_status_in_db,
)
from lakesource.postgres.lake_info_read import (
    count_source_hylak_ids_in_range,
    fetch_max_lake_info_hylak_id,
    fetch_source_hylak_ids_in_range,
)

from .schema import QuantileResult, RUN_STATUS_DONE, RUN_STATUS_ERROR

_VALID_RUN_STATUS = {RUN_STATUS_DONE, RUN_STATUS_ERROR}

_FETCH_TRANSITION_COUNTS_SQL = """
SELECT transition_type, COUNT(*)::bigint AS count
FROM quantile_abrupt_transitions
GROUP BY transition_type
"""

_FETCH_TRANSITION_SEASONALITY_SQL = """
SELECT to_month, COUNT(*)::bigint AS count
FROM quantile_abrupt_transitions
GROUP BY to_month
"""

_FETCH_LAKE_TRANSITION_COUNTS_SQL = """
SELECT hylak_id, COUNT(*)::bigint AS transition_count
FROM quantile_abrupt_transitions
GROUP BY hylak_id
"""

_FETCH_LAKE_EXTREME_COUNTS_SQL = """
SELECT hylak_id, COUNT(*)::bigint AS extreme_count
FROM quantile_extremes
GROUP BY hylak_id
"""

_FETCH_RUN_STATUS_COUNTS_SQL = """
SELECT status, COUNT(*)::bigint AS count
FROM quantile_run_status
GROUP BY status
"""

_COUNT_LABEL_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM quantile_labels
"""
_COUNT_EXTREME_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM quantile_extremes
"""
_COUNT_TRANSITION_ROWS_SQL = """
SELECT COUNT(*)::bigint AS count
FROM quantile_abrupt_transitions
"""


def ensure_quantile_tables(conn: psycopg.Connection) -> None:
    ensure_quantile_tables_in_db(conn)


def upsert_quantile_labels(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_quantile_labels_in_db(conn, rows, commit=commit)


def upsert_quantile_extremes(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_quantile_extremes_in_db(conn, rows, commit=commit)


def upsert_quantile_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_quantile_abrupt_transitions_in_db(
        conn,
        rows,
        commit=commit,
    )


def upsert_quantile_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_quantile_run_status_in_db(conn, rows, commit=commit)


def count_source_lakes_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> int:
    return count_source_hylak_ids_in_range(conn, chunk_start, chunk_end)


def count_processed_lakes_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> int:
    return count_quantile_status_in_range(
        conn,
        chunk_start,
        chunk_end,
    )


def fetch_source_hylak_ids_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> set[int]:
    return fetch_source_hylak_ids_in_range(conn, chunk_start, chunk_end)


def fetch_processed_hylak_ids_in_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
) -> set[int]:
    return fetch_quantile_status_ids_in_range(
        conn,
        chunk_start,
        chunk_end,
    )


def fetch_max_hylak_id(conn: psycopg.Connection) -> int:
    max_hylak_id = fetch_max_lake_info_hylak_id(conn)
    return 0 if max_hylak_id is None else int(max_hylak_id)


def result_to_label_rows(
    result: QuantileResult,
) -> list[dict]:
    columns = [
        "hylak_id",
        "year",
        "month",
        "water_area",
        "index_value",
        "threshold_low",
        "threshold_high",
        "extreme_label",
    ]
    df = result.labels_df.loc[:, columns].copy()
    df = df.rename(columns={
        "index_value": "anomaly",
        "threshold_low": "q_low",
        "threshold_high": "q_high",
    })
    df["monthly_climatology"] = df["anomaly"]
    return df.to_dict("records")


def result_to_extreme_rows(
    result: QuantileResult,
) -> list[dict]:
    if result.extremes_df.empty:
        return []
    df = result.extremes_df.copy()
    df = df.rename(columns={"index_value": "anomaly"})
    df["monthly_climatology"] = df["anomaly"]
    return df.to_dict("records")


def result_to_transition_rows(
    result: QuantileResult,
) -> list[dict]:
    if result.transitions_df.empty:
        return []
    df = result.transitions_df.copy()
    columns_map = {
        "from_index_value": "from_anomaly",
        "to_index_value": "to_anomaly",
    }
    df = df.rename(columns=columns_map)
    return df.to_dict("records")


def make_run_status_row(
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
    status: str,
    error_message: str | None = None,
) -> dict:
    if status not in _VALID_RUN_STATUS:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": int(hylak_id),
        "chunk_start": int(chunk_start),
        "chunk_end": int(chunk_end),
        "status": status,
        "error_message": None if error_message is None else str(error_message)[:500],
    }


def fetch_summary_cache_sources(
    conn: psycopg.Connection,
) -> dict[str, Any]:

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
        str(row["status"]): int(row["count"]) for _, row in status_counts.iterrows()
    }
    metadata = {
        "labels_rows": labels_count,
        "extremes_rows": extremes_count,
        "transitions_rows": transitions_count,
        "done_count": status_map.get(RUN_STATUS_DONE, 0),
        "error_count": status_map.get(RUN_STATUS_ERROR, 0),
    }
    return {
        "transition_counts": transition_counts,
        "transition_seasonality": transition_seasonality,
        "lake_transition_counts": lake_transition_counts,
        "lake_extreme_counts": lake_extreme_counts,
        "run_metadata": metadata,
    }
