"""DB adapter wrappers and row shapers for PWM extreme quantile outputs."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
import psycopg

from lakesource.postgres.lake import (
    ensure_pwm_extreme_tables as ensure_pwm_extreme_tables_in_db,
    upsert_pwm_extreme_run_status as upsert_pwm_extreme_run_status_in_db,
    upsert_pwm_extreme_thresholds as upsert_pwm_extreme_thresholds_in_db,
)

from .schema import PWMExtremeResult, RUN_STATUS_DONE, RUN_STATUS_ERROR

_VALID_RUN_STATUS = {RUN_STATUS_DONE, RUN_STATUS_ERROR}


def ensure_pwm_extreme_tables(conn: psycopg.Connection) -> None:
    ensure_pwm_extreme_tables_in_db(conn)


def upsert_pwm_extreme_thresholds(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_thresholds_in_db(conn, rows, commit=commit)


def upsert_pwm_extreme_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_run_status_in_db(conn, rows, commit=commit)


def result_to_threshold_rows(
    result: PWMExtremeResult,
    *,
    workflow_version: str,
) -> list[dict[str, Any]]:
    """Convert a PWMExtremeResult to DB row dicts for the thresholds table."""
    rows: list[dict[str, Any]] = []
    for mr in result.month_results:
        lambda_dict = {f"lambda_{j}": float(v) for j, v in enumerate(mr.lambda_opt)}
        pwm_dict = {f"b_{j}": float(v) for j, v in enumerate(mr.pwm_coefficients)}
        rows.append(
            {
                "hylak_id": mr.hylak_id,
                "month": mr.month,
                "mean_area": mr.mean_area,
                "epsilon": mr.epsilon,
                **lambda_dict,
                **pwm_dict,
                "threshold_high": mr.threshold_high,
                "threshold_low": mr.threshold_low,
                "converged": mr.converged,
                "objective_value": mr.objective_value,
                "workflow_version": workflow_version,
            }
        )
    return rows


def make_run_status_row(
    *,
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
    workflow_version: str,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    if status not in _VALID_RUN_STATUS:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": hylak_id,
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "workflow_version": workflow_version,
        "status": status,
        "error_message": error_message,
    }
