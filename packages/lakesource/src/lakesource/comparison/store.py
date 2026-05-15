"""DB adapter wrappers and row shapers for comparison experiment outputs."""

from __future__ import annotations

import psycopg

from lakesource.postgres.comparison_schema import ensure_comparison_tables as ensure_comparison_tables_in_db
from lakesource.postgres.comparison_schema import upsert_comparison_agreement as upsert_comparison_agreement_in_db
from lakesource.postgres.comparison_schema import upsert_comparison_run_status as upsert_comparison_run_status_in_db

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


def upsert_comparison_agreement(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
) -> None:
    """Upsert comparison agreement."""
    upsert_comparison_agreement_in_db(conn, rows, commit=commit)


def make_run_status_row(
    *,
    hylak_id: int,
    chunk_start: int,
    chunk_end: int,
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
        "status": status,
        "quantile_status": quantile_status,
        "pwm_status": pwm_status,
        "error_message": None if error_message is None else str(error_message)[:500],
    }


def make_comparison_agreement_row(
    hylak_id: int,
    quantile_labels_df,
    pwm_labels_df,
) -> dict:
    """Compute per-lake agreement metrics between quantile and pwm extreme labels."""
    q_df = quantile_labels_df.set_index(["year", "month"])
    p_df = pwm_labels_df.set_index(["year", "month"])
    merged = q_df.join(p_df, lsuffix="_q", rsuffix="_p", how="inner")

    def _is_high(s):
        return (s == "extreme_high").sum()

    def _is_low(s):
        return (s == "extreme_low").sum()

    def _is_normal(s):
        return (~s.isin(["extreme_high", "extreme_low"])).sum()

    n_months = len(merged)
    if n_months == 0:
        return {
            "hylak_id": int(hylak_id),
            "n_months": 0,
            "label_agree_rate": 0.0,
            "q_high_n": 0,
            "q_low_n": 0,
            "pwm_high_n": 0,
            "pwm_low_n": 0,
            "high_agree_n": 0,
            "low_agree_n": 0,
            "normal_agree_n": 0,
        }

    agree = merged["extreme_label_q"] == merged["extreme_label_p"]
    return {
        "hylak_id": int(hylak_id),
        "n_months": n_months,
        "label_agree_rate": float(agree.mean()),
        "q_high_n": int(_is_high(merged["extreme_label_q"])),
        "q_low_n": int(_is_low(merged["extreme_label_q"])),
        "pwm_high_n": int(_is_high(merged["extreme_label_p"])),
        "pwm_low_n": int(_is_low(merged["extreme_label_p"])),
        "high_agree_n": int(((merged["extreme_label_q"] == "extreme_high") & (merged["extreme_label_p"] == "extreme_high")).sum()),
        "low_agree_n": int(((merged["extreme_label_q"] == "extreme_low") & (merged["extreme_label_p"] == "extreme_low")).sum()),
        "normal_agree_n": int(_is_normal(merged["extreme_label_q"]) & _is_normal(merged["extreme_label_p"])),
    }
