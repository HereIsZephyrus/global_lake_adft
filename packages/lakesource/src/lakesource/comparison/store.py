"""DB adapter wrappers and row shapers for comparison experiment outputs."""

from __future__ import annotations

import psycopg

from lakesource.postgres.lake import (
    ensure_comparison_tables as ensure_comparison_tables_in_db,
    upsert_comparison_run_status as upsert_comparison_run_status_in_db,
)

from .schema import RUN_STATUS_DONE, RUN_STATUS_ERROR

_VALID_RUN_STATUS = {RUN_STATUS_DONE, RUN_STATUS_ERROR}


def ensure_comparison_tables(conn: psycopg.Connection) -> None:
    ensure_comparison_tables_in_db(conn)


def upsert_comparison_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    upsert_comparison_run_status_in_db(conn, rows, commit=commit)


def make_run_status_row(
    *,
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
    workflow_version: str,
    status: str,
    quantile_status: str | None = None,
    pwm_status: str | None = None,
    error_message: str | None = None,
) -> dict:
    if status not in _VALID_RUN_STATUS:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": int(hylak_id),
        "chunk_start": int(chunk_start),
        "chunk_end": int(chunk_end),
        "workflow_version": workflow_version.strip(),
        "status": status,
        "quantile_status": quantile_status,
        "pwm_status": pwm_status,
        "error_message": None if error_message is None else str(error_message)[:500],
    }