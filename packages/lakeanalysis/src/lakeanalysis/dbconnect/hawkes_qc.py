"""Read-only queries for Hawkes / EOT production QA (SERIES_DB).

Use with ``series_db.connection_context()`` and pandas for exploration or scripts.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import psycopg

log = logging.getLogger(__name__)

_HAWKES_QC_SUMMARY_BY_QUANTILE_SQL = """
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
FROM hawkes_results
GROUP BY threshold_quantile
ORDER BY threshold_quantile;
"""

_HAWKES_ERROR_MESSAGE_COUNTS_SQL = """
SELECT LEFT(COALESCE(error_message, ''), 200) AS error_prefix,
       COUNT(*)::bigint AS n
FROM hawkes_results
WHERE error_message IS NOT NULL AND TRIM(error_message) <> ''
GROUP BY 1
ORDER BY n DESC
LIMIT %(limit)s;
"""

_HAWKES_RESULTS_SELECT = """
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
FROM hawkes_results
"""

_HAWKES_LRT_SELECT = """
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
FROM hawkes_lrt
"""

_HAWKES_LRT_SUMMARY_BY_TEST_SQL = """
SELECT
    test_name,
    COUNT(*)::bigint AS n_rows,
    AVG(CASE WHEN reject_null THEN 1.0 ELSE 0.0 END) AS reject_null_rate,
    AVG(p_value) AS mean_p_value,
    MIN(p_value) AS min_p_value,
    MAX(p_value) AS max_p_value
FROM hawkes_lrt
WHERE 1=1
"""

_EOT_HAWKES_COVERAGE_SQL = """
WITH eot_counts AS (
    SELECT
        hylak_id,
        threshold_quantile,
        COUNT(DISTINCT tail)::int AS n_tails
    FROM eot_results
    GROUP BY hylak_id, threshold_quantile
)
SELECT
    h.threshold_quantile,
    COUNT(*)::bigint AS hawkes_rows,
    SUM(CASE WHEN ec.n_tails IS NULL THEN 1 ELSE 0 END)::bigint AS hawkes_without_eot_row,
    SUM(CASE WHEN ec.n_tails >= 1 THEN 1 ELSE 0 END)::bigint AS hawkes_with_any_eot,
    SUM(CASE WHEN ec.n_tails >= 2 THEN 1 ELSE 0 END)::bigint AS hawkes_with_both_eot_tails,
    AVG(CASE WHEN ec.n_tails >= 2 THEN 1.0 ELSE 0.0 END) AS frac_both_eot_tails
FROM hawkes_results h
LEFT JOIN eot_counts ec
    ON ec.hylak_id = h.hylak_id
   AND ec.threshold_quantile = h.threshold_quantile
WHERE 1=1
"""

_HAWKES_TRANSITION_MONTHLY_SELECT = """
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
FROM hawkes_transition_monthly
"""


def _append_quantile_filter(
    base_sql: str, params: dict[str, Any], threshold_quantile: float | None
) -> str:
    """Append AND threshold_quantile = %(threshold_quantile)s when set."""
    if threshold_quantile is None:
        return base_sql
    params["threshold_quantile"] = threshold_quantile
    return base_sql + " AND threshold_quantile = %(threshold_quantile)s"


def fetch_hawkes_qc_summary_by_quantile(conn: psycopg.Connection) -> pd.DataFrame:
    """Aggregate hawkes_results health metrics per threshold quantile.

    Returns:
        DataFrame with columns including n_rows, qc_pass_rate, converged_rate.
    """
    return pd.read_sql(_HAWKES_QC_SUMMARY_BY_QUANTILE_SQL, conn)


def fetch_hawkes_error_message_counts(
    conn: psycopg.Connection, *, limit: int = 30
) -> pd.DataFrame:
    """Top error_message prefixes by frequency (truncated to 200 chars).

    Args:
        conn: Open SERIES_DB connection.
        limit: Max distinct prefixes to return.

    Returns:
        DataFrame with columns [error_prefix, n].
    """
    return pd.read_sql(
        _HAWKES_ERROR_MESSAGE_COUNTS_SQL, conn, params={"limit": limit}
    )


def fetch_hawkes_results(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    qc_pass_only: bool = False,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load hawkes_results rows with optional filters.

    Args:
        conn: Open SERIES_DB connection.
        threshold_quantile: If set, filter to this quantile.
        qc_pass_only: If True, keep rows with qc_pass IS TRUE.
        limit: If set, append LIMIT (for large tables during exploration).

    Returns:
        Full or filtered hawkes_results as a DataFrame.
    """
    params: dict[str, Any] = {}
    sql = _HAWKES_RESULTS_SELECT + "\nWHERE 1=1"
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if qc_pass_only:
        sql += " AND qc_pass IS TRUE"
    sql += "\nORDER BY hylak_id, threshold_quantile"
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += "\nLIMIT %(limit_n)s"
    return pd.read_sql(sql, conn, params=params or None)


def fetch_hawkes_lrt(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    test_name: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load hawkes_lrt rows with optional filters."""
    params: dict[str, Any] = {}
    sql = _HAWKES_LRT_SELECT + "\nWHERE 1=1"
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if test_name is not None:
        params["test_name"] = test_name
        sql += " AND test_name = %(test_name)s"
    sql += "\nORDER BY hylak_id, threshold_quantile, test_name"
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += "\nLIMIT %(limit_n)s"
    return pd.read_sql(sql, conn, params=params or None)


def fetch_hawkes_lrt_summary_by_test(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
) -> pd.DataFrame:
    """Per test_name: row counts, reject_null rate, p_value summaries."""
    params: dict[str, Any] = {}
    sql = _HAWKES_LRT_SUMMARY_BY_TEST_SQL
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    sql += "\nGROUP BY test_name\nORDER BY test_name"
    return pd.read_sql(sql, conn, params=params or None)


def fetch_eot_hawkes_coverage(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
) -> pd.DataFrame:
    """Compare hawkes_results lakes to eot_results tail coverage per quantile.

    For each hawkes row, counts whether matching eot_results exist (0, 1, or 2+ tails).

    Args:
        conn: Open SERIES_DB connection.
        threshold_quantile: Optional filter on quantile.

    Returns:
        DataFrame grouped by threshold_quantile with coverage columns.
    """
    params: dict[str, Any] = {}
    sql = _EOT_HAWKES_COVERAGE_SQL
    if threshold_quantile is not None:
        params["threshold_quantile"] = threshold_quantile
        sql += " AND h.threshold_quantile = %(threshold_quantile)s"
    sql += "\nGROUP BY h.threshold_quantile\nORDER BY h.threshold_quantile"
    return pd.read_sql(sql, conn, params=params if params else None)


def fetch_hawkes_transition_monthly(
    conn: psycopg.Connection,
    *,
    threshold_quantile: float | None = None,
    hylak_id: int | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Load hawkes_transition_monthly with optional lake / quantile filters."""
    params: dict[str, Any] = {}
    sql = _HAWKES_TRANSITION_MONTHLY_SELECT + "\nWHERE 1=1"
    sql = _append_quantile_filter(sql, params, threshold_quantile)
    if hylak_id is not None:
        params["hylak_id"] = int(hylak_id)
        sql += " AND hylak_id = %(hylak_id)s"
    sql += "\nORDER BY hylak_id, threshold_quantile, year, month, direction"
    if limit is not None:
        params["limit_n"] = int(limit)
        sql += "\nLIMIT %(limit_n)s"
    return pd.read_sql(sql, conn, params=params or None)
