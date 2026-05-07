"""Database operations for PWM extreme quantile tables."""

from __future__ import annotations

from typing import Any
import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _ensure_pwm_extreme_thresholds_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    workflow_version    TEXT         NOT NULL,
    mean_area           DOUBLE PRECISION,
    epsilon             DOUBLE PRECISION,
    lambda_0            DOUBLE PRECISION,
    lambda_1            DOUBLE PRECISION,
    lambda_2            DOUBLE PRECISION,
    lambda_3            DOUBLE PRECISION,
    lambda_4            DOUBLE PRECISION,
    b_0                 DOUBLE PRECISION,
    b_1                 DOUBLE PRECISION,
    b_2                 DOUBLE PRECISION,
    b_3                 DOUBLE PRECISION,
    b_4                 DOUBLE PRECISION,
    threshold_high      DOUBLE PRECISION,
    threshold_low       DOUBLE PRECISION,
    converged           BOOLEAN,
    objective_value     DOUBLE PRECISION,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version, month)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_thresholds")))


def _ensure_pwm_extreme_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    workflow_version  TEXT         NOT NULL,
    chunk_start       INTEGER,
    chunk_end         INTEGER,
    status            TEXT         NOT NULL,
    error_message     TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version)
);
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_run_status")))


def _upsert_pwm_extreme_thresholds_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, month, workflow_version,
    mean_area, epsilon,
    lambda_0, lambda_1, lambda_2, lambda_3, lambda_4,
    b_0, b_1, b_2, b_3, b_4,
    threshold_high, threshold_low, converged, objective_value, computed_at
) VALUES (
    %(hylak_id)s, %(month)s, %(workflow_version)s,
    %(mean_area)s, %(epsilon)s,
    %(lambda_0)s, %(lambda_1)s, %(lambda_2)s, %(lambda_3)s, %(lambda_4)s,
    %(b_0)s, %(b_1)s, %(b_2)s, %(b_3)s, %(b_4)s,
    %(threshold_high)s, %(threshold_low)s, %(converged)s, %(objective_value)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    mean_area       = EXCLUDED.mean_area,
    epsilon         = EXCLUDED.epsilon,
    lambda_0        = EXCLUDED.lambda_0,
    lambda_1        = EXCLUDED.lambda_1,
    lambda_2        = EXCLUDED.lambda_2,
    lambda_3        = EXCLUDED.lambda_3,
    lambda_4        = EXCLUDED.lambda_4,
    b_0             = EXCLUDED.b_0,
    b_1             = EXCLUDED.b_1,
    b_2             = EXCLUDED.b_2,
    b_3             = EXCLUDED.b_3,
    b_4             = EXCLUDED.b_4,
    threshold_high  = EXCLUDED.threshold_high,
    threshold_low   = EXCLUDED.threshold_low,
    converged       = EXCLUDED.converged,
    objective_value = EXCLUDED.objective_value,
    computed_at     = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_thresholds")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "workflow_version", "month")
        ),
    )


def _upsert_pwm_extreme_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, chunk_start, chunk_end, status, error_message, computed_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(
        table=sql.Identifier(tc.series_table("pwm_extreme_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "workflow_version")
        ),
    )


def _count_pwm_extreme_status_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT COUNT(*)
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
  AND status = 'done'
  AND workflow_version = %(workflow_version)s
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_run_status")))


def _fetch_pwm_extreme_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
  AND status = 'done'
  AND workflow_version = %(workflow_version)s
""").format(table=sql.Identifier(tc.series_table("pwm_extreme_run_status")))


def ensure_pwm_extreme_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create PWM extreme quantile tables if they do not exist."""
    with conn.cursor() as cur:
        cur.execute(_ensure_pwm_extreme_thresholds_table_sql(table_config))
        cur.execute(_ensure_pwm_extreme_status_table_sql(table_config))
    conn.commit()


def upsert_pwm_extreme_thresholds(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme threshold rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_thresholds_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_thresholds row(s)", len(rows))


def upsert_pwm_extreme_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert PWM extreme run status rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_pwm_extreme_status_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d pwm_extreme_run_status row(s)", len(rows))


def count_pwm_extreme_status_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
    table_config: TableConfig = _default_table_config,
) -> int:
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "workflow_version": workflow_version,
    }
    with conn.cursor() as cur:
        cur.execute(
            _count_pwm_extreme_status_in_range_sql(table_config),
            params,
        )
        return int(cur.fetchone()[0])


def fetch_pwm_extreme_status_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    workflow_version: str,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "workflow_version": workflow_version,
    }
    with conn.cursor() as cur:
        cur.execute(
            _fetch_pwm_extreme_status_ids_in_range_sql(table_config),
            params,
        )
        return {int(row[0]) for row in cur.fetchall()}
