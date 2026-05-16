"""DB adapter wrappers and row shapers for PWM extreme quantile outputs."""

from __future__ import annotations

from typing import Any

import psycopg

from lakesource.postgres.lake_pwm import (
    ensure_pwm_extreme_tables as ensure_pwm_extreme_tables_in_db,
    upsert_pwm_extreme_labels as upsert_pwm_extreme_labels_in_db,
    upsert_pwm_extreme_extremes as upsert_pwm_extreme_extremes_in_db,
    upsert_pwm_extreme_return_levels as upsert_pwm_extreme_return_levels_in_db,
    upsert_pwm_extreme_abrupt_transitions as upsert_pwm_extreme_abrupt_transitions_in_db,
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


def upsert_pwm_extreme_labels(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_labels_in_db(conn, rows, commit=commit)


def upsert_pwm_extreme_extremes(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_extremes_in_db(conn, rows, commit=commit)


def upsert_pwm_extreme_return_levels(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_return_levels_in_db(conn, rows, commit=commit)


def upsert_pwm_extreme_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_abrupt_transitions_in_db(conn, rows, commit=commit)


def upsert_pwm_extreme_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    commit: bool = True,
) -> None:
    upsert_pwm_extreme_run_status_in_db(conn, rows, commit=commit)


def result_to_threshold_rows(
    result: PWMExtremeResult,
) -> list[dict[str, Any]]:
    """Convert a PWMExtremeResult to DB row dicts for the thresholds table."""
    rows: list[dict[str, Any]] = []
    for mr in result.month_results:
        lambda_dict = {f"lambda_{j}": float(v) for j, v in enumerate(mr.lambda_opt)}
        pwm_dict = {f"b_{j}": float(v) for j, v in enumerate(mr.pwm_coefficients)}
        rows.append(
            {
                "hylak_id": mr.hylak_id,
                "threshold_quantile": mr.threshold_quantile,
                "month": mr.month,
                "mean_area": mr.mean_area,
                "epsilon": mr.epsilon,
                **lambda_dict,
                **pwm_dict,
                "threshold_high": mr.threshold_high,
                "threshold_low": mr.threshold_low,
                "converged": mr.converged,
                "objective_value": mr.objective_value,
            }
        )
    return rows


def result_to_label_rows(
    result: PWMExtremeResult,
) -> list[dict[str, Any]]:
    """Convert PWMExtremeResult.labels_df to DB row dicts for the labels table."""
    threshold_quantile = result.threshold_quantile
    rows = result.labels_df.loc[:, [
        "hylak_id",
        "year",
        "month",
        "water_area",
        "index_value",
        "threshold_low",
        "threshold_high",
        "extreme_label",
    ]].to_dict("records")
    for row in rows:
        row["threshold_quantile"] = threshold_quantile
    return rows


def result_to_extreme_rows(
    result: PWMExtremeResult,
) -> list[dict[str, Any]]:
    """Convert PWMExtremeResult.extremes_df to DB row dicts for the extremes table."""
    if result.extremes_df.empty:
        return []
    threshold_quantile = result.threshold_quantile
    rows = result.extremes_df.loc[:, [
        "hylak_id",
        "year",
        "month",
        "event_type",
        "water_area",
        "index_value",
        "threshold",
        "severity",
        "extreme_label",
    ]].to_dict("records")
    for row in rows:
        row["threshold_quantile"] = threshold_quantile
    return rows


def result_to_transition_rows(
    result: PWMExtremeResult,
) -> list[dict[str, Any]]:
    """Convert PWMExtremeResult.transitions_df to DB row dicts for the transitions table."""
    if result.transitions_df.empty:
        return []
    threshold_quantile = result.threshold_quantile
    rows = result.transitions_df.loc[:, [
        "hylak_id",
        "from_year",
        "from_month",
        "to_year",
        "to_month",
        "transition_type",
        "from_water_area",
        "to_water_area",
        "from_label",
        "to_label",
    ]].to_dict("records")
    for row in rows:
        row["threshold_quantile"] = threshold_quantile
    return rows


def return_levels_to_rows(
    hylak_id: int,
    summary_df,
    *,
    threshold_quantile: float | None = None,
) -> list[dict[str, Any]]:
    """Convert EVT summary rows to DB rows for pwm_extreme_return_levels."""
    if summary_df is None or summary_df.empty:
        return []
    rows: list[dict[str, Any]] = []
    for _, row in summary_df.iterrows():
        rows.append(
            {
                "hylak_id": int(hylak_id),
                "threshold_quantile": threshold_quantile,
                "tail": str(row["tail"]),
                "return_period": int(row["return_period"]),
                "return_level": _maybe_float(row["return_level"]),
                "shape": _maybe_float(row["shape"]),
                "scale": _maybe_float(row["scale"]),
                "threshold": _maybe_float(row["threshold"]),
                "n_total": int(row["n_total"]),
                "n_exceedances": int(row["n_exceedances"]),
                "converged": bool(row["converged"]),
                "error_message": None if row["error_message"] is None else str(row["error_message"]),
                "evt_route": str(row["evt_route"]),
                "strength_unit": str(row["strength_unit"]),
            }
        )
    return rows


def _maybe_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    return value


def make_run_status_row(
    *,
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
    status: str,
    error_message: str | None = None,
) -> dict[str, Any]:
    if status not in _VALID_RUN_STATUS:
        raise ValueError(f"Invalid run status: {status!r}")
    return {
        "hylak_id": hylak_id,
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "status": status,
        "error_message": error_message,
    }
