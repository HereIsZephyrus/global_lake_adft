"""Read-only queries for Hawkes / EOT production QA (SERIES_DB).

Use with ``series_db.connection_context()`` and pandas for exploration or scripts.

Supports both PWM-Hawkes and EOT-Hawkes table sets.  Legacy unified
``fetch_hawkes_results`` etc. delegates to PWM variants.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import psycopg
from psycopg import sql as psql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


# ------------------------------------------------------------------
# SQL template builders — parametrised by logical table name
# ------------------------------------------------------------------

def _hawkes_qc_summary_by_quantile_sql(
    tc: TableConfig, logical_name: str
) -> psql.Composed:
    return psql.SQL("""
SELECT
    threshold_quantile,
    COUNT(*)::bigint AS n_rows,
    AVG(CASE WHEN qc_pass THEN 1.0 ELSE 0.0 END) AS qc_pass_rate,
    AVG(CASE WHEN converged THEN 1.0 ELSE 0.0 END) AS converged_rate,
    AVG(
        CASE
            WHEN error_message IS NULL OR TRIM(error_message) = '' THEN 1.0
            ELSE 0.0
        END
    ) AS no_error_message_rate,
    MIN(computed_at) AS computed_at_min,
    MAX(computed_at) AS computed_at_max
FROM {table}
GROUP BY threshold_quantile
ORDER BY threshold_quantile;
""").format(table=psql.Identifier(tc.series_table(logical_name)))


def _hawkes_error_message_counts_sql(
    tc: TableConfig, logical_name: str
) -> psql.Composed:
    return psql.SQL("""
SELECT LEFT(COALESCE(error_message, ''), 200) AS error_prefix,
       COUNT(*)::bigint AS n
FROM {table}
WHERE error_message IS NOT NULL AND TRIM(error_message) <> ''
GROUP BY 1
ORDER BY n DESC
LIMIT %(limit)s;
""").format(table=psql.Identifier(tc.series_table(logical_name)))


def _hawkes_results_select(
    tc: TableConfig, logical_name: str
) -> psql.Composed:
    return psql.SQL("""
SELECT
    hylak_id,
    threshold_quantile,
    converged,
    log_likelihood,
    objective_value,
    n_events,
    n_dry_events,
    n_wet_events,
    mu_d,
    mu_w,
    alpha_dd,
    alpha_dw,
    alpha_wd,
    alpha_ww,
    beta_dd,
    beta_dw,
    beta_wd,
    beta_ww,
    spectral_radius,
    lrt_p_d_to_w,
    lrt_p_w_to_d,
    qc_pass,
    qc_exceedance_rate,
    qc_relative_amplitude,
    qc_median_excess,
    error_message,
    computed_at
FROM {table}
""").format(table=psql.Identifier(tc.series_table(logical_name)))


def _hawkes_lrt_select(
    tc: TableConfig, logical_name: str
) -> psql.Composed:
    return psql.SQL("""
SELECT
    hylak_id,
    threshold_quantile,
    test_name,
    lr_statistic,
    df,
    p_value,
    significance_level,
    reject_null,
    restricted_log_likelihood,
    full_log_likelihood,
    computed_at
FROM {table}
""").format(table=psql.Identifier(tc.series_table(logical_name)))


def _hawkes_lrt_summary_by_test_sql(
    tc: TableConfig, logical_name: str
) -> psql.Composed:
    return psql.SQL("""
SELECT
    test_name,
    COUNT(*)::bigint AS n_rows,
    AVG(CASE WHEN reject_null THEN 1.0 ELSE 0.0 END) AS reject_null_rate,
    AVG(p_value) AS mean_p_value,
    MIN(p_value) AS min_p_value,
    MAX(p_value) AS max_p_value
FROM {table}
WHERE 1=1
""").format(table=psql.Identifier(tc.series_table(logical_name)))


def _eot_hawkes_coverage_sql(tc: TableConfig) -> psql.Composed:
    """Coverage of eot_results vs eot_hawkes_results."""
    return psql.SQL("""
WITH eot_counts AS (
    SELECT
        hylak_id,
        threshold_quantile,
        COUNT(DISTINCT tail)::int AS n_tails
    FROM {eot_results}
    GROUP BY hylak_id, threshold_quantile
)
SELECT
    h.threshold_quantile,
    COUNT(*)::bigint AS hawkes_rows,
    SUM(CASE WHEN ec.n_tails IS NULL THEN 1 ELSE 0 END)::bigint AS hawkes_without_eot_row,
    SUM(CASE WHEN ec.n_tails >= 1 THEN 1 ELSE 0 END)::bigint AS hawkes_with_any_eot,
    SUM(CASE WHEN ec.n_tails >= 2 THEN 1 ELSE 0 END)::bigint AS hawkes_with_both_eot_tails,
    AVG(CASE WHEN ec.n_tails >= 2 THEN 1.0 ELSE 0.0 END) AS frac_both_eot_tails
FROM {hawkes_table} h
LEFT JOIN eot_counts ec
    ON ec.hylak_id = h.hylak_id
   AND ec.threshold_quantile = h.threshold_quantile
WHERE 1=1
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
        hawkes_table=psql.Identifier(tc.series_table("eot_hawkes_results")),
    )


def _hawkes_transition_monthly_select(
    tc: TableConfig, logical_name: str
) -> psql.Composed:
    return psql.SQL("""
SELECT
    hylak_id,
    threshold_quantile,
    year,
    month,
    direction,
    score_raw,
    score_norm,
    significance_quantile,
    significance_threshold,
    significant,
    computed_at
FROM {table}
""").format(table=psql.Identifier(tc.series_table(logical_name)))


# ------------------------------------------------------------------
# Filter helper
# ------------------------------------------------------------------

def _append_quantile_filter(
    base_sql: psql.Composed, params: dict[str, Any], threshold_quantile: float | None
) -> psql.Composed:
    if threshold_quantile is None:
        return base_sql
    params["threshold_quantile"] = threshold_quantile
    return base_sql + psql.SQL(" AND threshold_quantile = %(threshold_quantile)s")


# ------------------------------------------------------------------
# PWM-Hawkes QC fetch functions
# ------------------------------------------------------------------

def fetch_pwm_hawkes_qc_summary_by_quantile(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    return pd.read_sql(
        _hawkes_qc_summary_by_quantile_sql(
            table_config, "pwm_hawkes_results"
        ).as_string(),
        conn,
    )


def fetch_pwm_hawkes_error_message_counts(
    conn: psycopg.Connection,
    *,
    limit: int = 30,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    return pd.read_sql(
        _hawkes_error_message_counts_sql(
            table_config, "pwm_hawkes_results"
        ).as_string(),
        conn,
        params={"limit": limit},
    )


def fetch_pwm_hawkes_results(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    qc_pass_only: bool = False,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_results_select(table_config, "pwm_hawkes_results") + psql.SQL("\nWHERE 1=1")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if qc_pass_only:
        sql += psql.SQL(" AND qc_pass IS TRUE")
    sql += psql.SQL("\nORDER BY hylak_id, threshold_quantile")
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += psql.SQL("\nLIMIT %(limit_n)s")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


def fetch_pwm_hawkes_lrt(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    test_name: str | None = None,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_lrt_select(table_config, "pwm_hawkes_lrt") + psql.SQL("\nWHERE 1=1")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if test_name is not None:
        params["test_name"] = test_name
        sql += psql.SQL(" AND test_name = %(test_name)s")
    sql += psql.SQL("\nORDER BY hylak_id, threshold_quantile, test_name")
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += psql.SQL("\nLIMIT %(limit_n)s")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


def fetch_pwm_hawkes_lrt_summary_by_test(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_lrt_summary_by_test_sql(table_config, "pwm_hawkes_lrt")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    sql += psql.SQL("\nGROUP BY test_name\nORDER BY test_name")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


def fetch_pwm_hawkes_transition_monthly(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    hylak_id: int | None = None,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_transition_monthly_select(
        table_config, "pwm_hawkes_transition_monthly"
    ) + psql.SQL("\nWHERE 1=1")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if hylak_id is not None:
        params["hylak_id"] = int(hylak_id)
        sql += psql.SQL(" AND hylak_id = %(hylak_id)s")
    sql += psql.SQL("\nORDER BY hylak_id, threshold_quantile, year, month, direction")
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += psql.SQL("\nLIMIT %(limit_n)s")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


# ------------------------------------------------------------------
# EOT-Hawkes QC fetch functions
# ------------------------------------------------------------------

def fetch_eot_hawkes_qc_summary_by_quantile(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    return pd.read_sql(
        _hawkes_qc_summary_by_quantile_sql(
            table_config, "eot_hawkes_results"
        ).as_string(),
        conn,
    )


def fetch_eot_hawkes_error_message_counts(
    conn: psycopg.Connection,
    *,
    limit: int = 30,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    return pd.read_sql(
        _hawkes_error_message_counts_sql(
            table_config, "eot_hawkes_results"
        ).as_string(),
        conn,
        params={"limit": limit},
    )


def fetch_eot_hawkes_results(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    qc_pass_only: bool = False,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_results_select(table_config, "eot_hawkes_results") + psql.SQL("\nWHERE 1=1")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if qc_pass_only:
        sql += psql.SQL(" AND qc_pass IS TRUE")
    sql += psql.SQL("\nORDER BY hylak_id, threshold_quantile")
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += psql.SQL("\nLIMIT %(limit_n)s")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


def fetch_eot_hawkes_lrt(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    test_name: str | None = None,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_lrt_select(table_config, "eot_hawkes_lrt") + psql.SQL("\nWHERE 1=1")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if test_name is not None:
        params["test_name"] = test_name
        sql += psql.SQL(" AND test_name = %(test_name)s")
    sql += psql.SQL("\nORDER BY hylak_id, threshold_quantile, test_name")
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += psql.SQL("\nLIMIT %(limit_n)s")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


def fetch_eot_hawkes_lrt_summary_by_test(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_lrt_summary_by_test_sql(table_config, "eot_hawkes_lrt")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    sql += psql.SQL("\nGROUP BY test_name\nORDER BY test_name")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


def fetch_eot_hawkes_transition_monthly(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    hylak_id: int | None = None,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    params: dict[str, Any] = {}
    sql = _hawkes_transition_monthly_select(
        table_config, "eot_hawkes_transition_monthly"
    ) + psql.SQL("\nWHERE 1=1")
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if hylak_id is not None:
        params["hylak_id"] = int(hylak_id)
        sql += psql.SQL(" AND hylak_id = %(hylak_id)s")
    sql += psql.SQL("\nORDER BY hylak_id, threshold_quantile, year, month, direction")
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += psql.SQL("\nLIMIT %(limit_n)s")
    return pd.read_sql(sql.as_string(), conn, params=params or None)


# ------------------------------------------------------------------
# EOT-Hawkes coverage (already uses eot_hawkes_results)
# ------------------------------------------------------------------

def fetch_eot_hawkes_coverage(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Compare eot_hawkes_results lakes to eot_results tail coverage per quantile.

    For each hawkes row, counts whether matching eot_results exist (0, 1, or 2+ tails).

    Args:
        conn: Open SERIES_DB connection.
        threshold_quantile: Optional filter on quantile.

    Returns:
        DataFrame grouped by threshold_quantile with coverage columns.
    """
    params: dict[str, Any] = {}
    sql = _eot_hawkes_coverage_sql(table_config)
    if threshold_quantile is not None:
        params["threshold_quantile"] = threshold_quantile
        sql += psql.SQL(" AND h.threshold_quantile = %(threshold_quantile)s")
    sql += psql.SQL("\nGROUP BY h.threshold_quantile\nORDER BY h.threshold_quantile")
    return pd.read_sql(sql.as_string(), conn, params=params if params else None)


# ------------------------------------------------------------------
# Deprecated: legacy unified fetch functions (delegate to PWM)
# ------------------------------------------------------------------

def fetch_hawkes_qc_summary_by_quantile(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Legacy — delegates to PWM-Hawkes tables."""
    return fetch_pwm_hawkes_qc_summary_by_quantile(conn, table_config=table_config)


def fetch_hawkes_error_message_counts(
    conn: psycopg.Connection,
    *,
    limit: int = 30,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Legacy — delegates to PWM-Hawkes tables."""
    return fetch_pwm_hawkes_error_message_counts(
        conn, limit=limit, table_config=table_config
    )


def fetch_hawkes_results(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    qc_pass_only: bool = False,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Legacy — delegates to PWM-Hawkes tables."""
    return fetch_pwm_hawkes_results(
        conn,
        threshold_quantile=threshold_quantile,
        qc_pass_only=qc_pass_only,
        limit=limit,
        table_config=table_config,
    )


def fetch_hawkes_lrt(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    test_name: str | None = None,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Legacy — delegates to PWM-Hawkes tables."""
    return fetch_pwm_hawkes_lrt(
        conn,
        threshold_quantile=threshold_quantile,
        test_name=test_name,
        limit=limit,
        table_config=table_config,
    )


def fetch_hawkes_lrt_summary_by_test(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Legacy — delegates to PWM-Hawkes tables."""
    return fetch_pwm_hawkes_lrt_summary_by_test(
        conn, threshold_quantile=threshold_quantile, table_config=table_config
    )


def fetch_hawkes_transition_monthly(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    hylak_id: int | None = None,
    limit: int | None = None,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Legacy — delegates to PWM-Hawkes tables."""
    return fetch_pwm_hawkes_transition_monthly(
        conn,
        threshold_quantile=threshold_quantile,
        hylak_id=hylak_id,
        limit=limit,
        table_config=table_config,
    )
