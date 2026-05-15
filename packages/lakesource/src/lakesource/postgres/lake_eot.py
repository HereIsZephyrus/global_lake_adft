"""Database operations for EOT (Excess Over Threshold) tables."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
import logging

import pandas as pd
import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()

EOT_RESULT_COLUMNS = (
    "hylak_id",
    "tail",
    "threshold_quantile",
    "converged",
    "log_likelihood",
    "threshold",
    "n_extremes",
    "n_observations",
    "n_frozen_months",
    "beta0",
    "beta1",
    "sin_1",
    "cos_1",
    "sigma",
    "xi",
    "error_message",
)


def _fetch_eot_extremes_by_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       tail,
       threshold_quantile,
       cluster_id,
       cluster_size,
       year,
       month,
       water_area,
       threshold_at_event,
       time,
       value
FROM {table}
WHERE hylak_id = %(hylak_id)s
ORDER BY year, month, tail, cluster_id
""").format(table=sql.Identifier(tc.series_table("eot_extremes")))


def _fetch_eot_extremes_by_id_and_q_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       tail,
       threshold_quantile,
       cluster_id,
       cluster_size,
       year,
       month,
       water_area,
       threshold_at_event,
       time,
       value
FROM {table}
WHERE hylak_id = %(hylak_id)s
  AND threshold_quantile = %(threshold_quantile)s
ORDER BY year, month, tail, cluster_id
""").format(table=sql.Identifier(tc.series_table("eot_extremes")))


def _ensure_eot_results_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id           INTEGER      NOT NULL,
    tail               TEXT         NOT NULL,
    threshold_quantile NUMERIC(5,4) NOT NULL,
    converged          BOOLEAN,
    log_likelihood     DOUBLE PRECISION,
    threshold          DOUBLE PRECISION,
    n_extremes         INTEGER,
    n_observations     INTEGER,
    n_frozen_months    INTEGER,
    beta0              DOUBLE PRECISION,
    beta1              DOUBLE PRECISION,
    sin_1              DOUBLE PRECISION,
    cos_1              DOUBLE PRECISION,
    sigma              DOUBLE PRECISION,
    xi                 DOUBLE PRECISION,
    error_message      TEXT,
    computed_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, tail, threshold_quantile)
);
""").format(table=sql.Identifier(tc.series_table("eot_results")))


def _ensure_eot_extremes_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id           INTEGER      NOT NULL,
    tail               TEXT         NOT NULL,
    threshold_quantile NUMERIC(5,4) NOT NULL,
    cluster_id         INTEGER      NOT NULL,
    cluster_size       INTEGER,
    year               INTEGER,
    month              INTEGER,
    water_area         DOUBLE PRECISION,
    threshold_at_event DOUBLE PRECISION,
    time               DOUBLE PRECISION,
    value              DOUBLE PRECISION,
    computed_at        TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, tail, threshold_quantile, cluster_id)
);
""").format(table=sql.Identifier(tc.series_table("eot_extremes")))


def _ensure_eot_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id         BIGINT        NOT NULL,
    chunk_start      BIGINT        NOT NULL,
    chunk_end        BIGINT        NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    error_message    TEXT,
    created_at       TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("eot_run_status")))


def _ensure_eot_return_levels_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id             INTEGER      NOT NULL,
    tail                 TEXT         NOT NULL,
    threshold_quantile   NUMERIC(5,4) NOT NULL,
    return_period_years  DOUBLE PRECISION NOT NULL,
    return_level         DOUBLE PRECISION,
    standard_error       DOUBLE PRECISION,
    ci_lower             DOUBLE PRECISION,
    ci_upper             DOUBLE PRECISION,
    computed_at          TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, tail, threshold_quantile, return_period_years)
);
""").format(table=sql.Identifier(tc.series_table("eot_return_levels")))


def _upsert_eot_results_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, tail, threshold_quantile,
    converged, log_likelihood, threshold,
    n_extremes, n_observations, n_frozen_months,
    beta0, beta1, sin_1, cos_1, sigma, xi,
    error_message, computed_at
) VALUES (
    %(hylak_id)s, %(tail)s, %(threshold_quantile)s,
    %(converged)s, %(log_likelihood)s, %(threshold)s,
    %(n_extremes)s, %(n_observations)s, %(n_frozen_months)s,
    %(beta0)s, %(beta1)s, %(sin_1)s, %(cos_1)s, %(sigma)s, %(xi)s,
    %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    converged          = EXCLUDED.converged,
    log_likelihood     = EXCLUDED.log_likelihood,
    threshold          = EXCLUDED.threshold,
    n_extremes         = EXCLUDED.n_extremes,
    n_observations     = EXCLUDED.n_observations,
    n_frozen_months    = EXCLUDED.n_frozen_months,
    beta0              = EXCLUDED.beta0,
    beta1              = EXCLUDED.beta1,
    sin_1              = EXCLUDED.sin_1,
    cos_1              = EXCLUDED.cos_1,
    sigma              = EXCLUDED.sigma,
    xi                 = EXCLUDED.xi,
    error_message      = EXCLUDED.error_message,
    computed_at        = now();
""").format(
        table=sql.Identifier(tc.series_table("eot_results")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "tail", "threshold_quantile")
        ),
    )


def _upsert_eot_extremes_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, tail, threshold_quantile, cluster_id,
    cluster_size, year, month, water_area, threshold_at_event,
    time, value, computed_at
) VALUES (
    %(hylak_id)s, %(tail)s, %(threshold_quantile)s, %(cluster_id)s,
    %(cluster_size)s, %(year)s, %(month)s, %(water_area)s, %(threshold_at_event)s,
    %(time)s, %(value)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    cluster_size       = EXCLUDED.cluster_size,
    year               = EXCLUDED.year,
    month              = EXCLUDED.month,
    water_area         = EXCLUDED.water_area,
    threshold_at_event = EXCLUDED.threshold_at_event,
    time               = EXCLUDED.time,
    value              = EXCLUDED.value,
    computed_at        = now();
""").format(
        table=sql.Identifier(tc.series_table("eot_extremes")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in ("hylak_id", "tail", "threshold_quantile", "cluster_id")
        ),
    )


def _upsert_eot_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, chunk_start, chunk_end, status, error_message, created_at
) VALUES (
    %(hylak_id)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    created_at    = now();
""").format(
        table=sql.Identifier(tc.series_table("eot_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id",)
        ),
    )


def _upsert_eot_return_levels_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, tail, threshold_quantile, return_period_years,
    return_level, standard_error, ci_lower, ci_upper, computed_at
) VALUES (
    %(hylak_id)s, %(tail)s, %(threshold_quantile)s, %(return_period_years)s,
    %(return_level)s, %(standard_error)s, %(ci_lower)s, %(ci_upper)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    return_level    = EXCLUDED.return_level,
    standard_error  = EXCLUDED.standard_error,
    ci_lower        = EXCLUDED.ci_lower,
    ci_upper        = EXCLUDED.ci_upper,
    computed_at     = now();
""").format(
        table=sql.Identifier(tc.series_table("eot_return_levels")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in ("hylak_id", "tail", "threshold_quantile", "return_period_years")
        ),
    )


def fetch_eot_extremes_by_id(
    conn: psycopg.Connection,
    hylak_id: int,
    threshold_quantile: float | None = None,
    *,
    table_config: TableConfig = _default_table_config,
) -> pd.DataFrame:
    """Fetch EOT extreme rows for one lake, optionally filtered by threshold quantile.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        hylak_id: Target lake id.
        threshold_quantile: Optional quantile filter (e.g., 0.95). If None, returns
            all quantiles available for the lake.
        table_config: Table name configuration.

    Returns:
        DataFrame with columns:
            [hylak_id, tail, threshold_quantile, cluster_id, cluster_size,
             year, month, water_area, threshold_at_event].
        Returns an empty DataFrame when no rows are found.
    """
    params: dict[str, int | Decimal] = {"hylak_id": int(hylak_id)}
    query = _fetch_eot_extremes_by_id_sql(table_config)
    if threshold_quantile is not None:
        try:
            params["threshold_quantile"] = Decimal(str(threshold_quantile))
        except (InvalidOperation, ValueError) as err:
            raise ValueError(
                f"threshold_quantile must be numeric, got: {threshold_quantile!r}"
            ) from err
        query = _fetch_eot_extremes_by_id_and_q_sql(table_config)
    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return df

    for col in ("hylak_id", "cluster_id", "cluster_size", "year", "month"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ("threshold_quantile", "water_area", "threshold_at_event"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "tail" in df.columns:
        df["tail"] = df["tail"].astype(str)

    return df


def ensure_eot_results_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create the EOT tables in SERIES_DB if they do not exist."""
    with conn.cursor() as cur:
        cur.execute(_ensure_eot_results_table_sql(table_config))
        cur.execute(_ensure_eot_extremes_table_sql(table_config))
        cur.execute(_ensure_eot_run_status_table_sql(table_config))
        cur.execute(_ensure_eot_return_levels_table_sql(table_config))
    conn.commit()
    log.debug("Ensured EOT tables exist")


def upsert_eot_results(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Insert or update EOT fit result rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_eot_results_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d eot_results row(s)", len(rows))


def upsert_eot_extremes(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Insert or update EOT extreme-event rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_eot_extremes_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d eot_extremes row(s)", len(rows))


def upsert_eot_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Insert or update EOT run status rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_eot_run_status_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d eot_run_status row(s)", len(rows))


def upsert_eot_return_levels(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_eot_return_levels_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d eot_return_levels row(s)", len(rows))
