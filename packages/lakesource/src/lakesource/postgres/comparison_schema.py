"""Database operations for comparison_run_status table."""
from __future__ import annotations
from typing import Any
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE
_default_table_config = TableConfig.default()







def _ensure_comparison_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id         BIGINT        NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    quantile_status  VARCHAR(16),
    pwm_status       VARCHAR(16),
    chunk_start      INT,
    chunk_end        INT,
    error_message    TEXT,
    created_at       TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("comparison_run_status")))

def _upsert_comparison_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, status, quantile_status, pwm_status,
    chunk_start, chunk_end, error_message, created_at
) VALUES (
    %(hylak_id)s, %(status)s, %(quantile_status)s, %(pwm_status)s,
    %(chunk_start)s, %(chunk_end)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    status          = EXCLUDED.status,
    quantile_status = EXCLUDED.quantile_status,
    pwm_status      = EXCLUDED.pwm_status,
    chunk_start     = EXCLUDED.chunk_start,
    chunk_end       = EXCLUDED.chunk_end,
    error_message   = EXCLUDED.error_message,
    created_at      = now();
""").format(
        table=sql.Identifier(tc.series_table("comparison_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id",)
        ),
    )

def _fetch_comparison_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id
FROM {{table}}
WHERE {CHUNK_WHERE}
  AND status = 'done'
""").format(table=sql.Identifier(tc.series_table("comparison_run_status")))

def _ensure_comparison_agreement_table_sql(tc: TableConfig) -> sql.Composed:
    """ ensure comparison agreement table sql."""
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id         BIGINT        NOT NULL,
    n_months         INT,
    label_agree_rate DOUBLE PRECISION,
    q_high_n         INT,
    q_low_n          INT,
    pwm_high_n       INT,
    pwm_low_n        INT,
    high_agree_n     INT,
    low_agree_n      INT,
    normal_agree_n   INT,
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("comparison_agreement")))


def _upsert_comparison_agreement_sql(tc: TableConfig) -> sql.Composed:
    """ upsert comparison agreement sql."""
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, n_months, label_agree_rate,
    q_high_n, q_low_n, pwm_high_n, pwm_low_n,
    high_agree_n, low_agree_n, normal_agree_n
) VALUES (
    %(hylak_id)s, %(n_months)s, %(label_agree_rate)s,
    %(q_high_n)s, %(q_low_n)s, %(pwm_high_n)s, %(pwm_low_n)s,
    %(high_agree_n)s, %(low_agree_n)s, %(normal_agree_n)s
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    n_months         = EXCLUDED.n_months,
    label_agree_rate = EXCLUDED.label_agree_rate,
    q_high_n         = EXCLUDED.q_high_n,
    q_low_n          = EXCLUDED.q_low_n,
    pwm_high_n       = EXCLUDED.pwm_high_n,
    pwm_low_n        = EXCLUDED.pwm_low_n,
    high_agree_n     = EXCLUDED.high_agree_n,
    low_agree_n      = EXCLUDED.low_agree_n,
    normal_agree_n   = EXCLUDED.normal_agree_n;
""").format(
        table=sql.Identifier(tc.series_table("comparison_agreement")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id",)
        ),
    )


def ensure_comparison_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_comparison_run_status_table_sql(table_config))
        cur.execute(_ensure_comparison_agreement_table_sql(table_config))
    conn.commit()

def upsert_comparison_run_status(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_comparison_run_status_sql(table_config), rows)
    if commit:
        conn.commit()


def upsert_comparison_agreement(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    """Upsert comparison agreement."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_comparison_agreement_sql(table_config), rows)
    if commit:
        conn.commit()


def fetch_comparison_status_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
    }
    with conn.cursor() as cur:
        cur.execute(
            _fetch_comparison_status_ids_in_range_sql(table_config),
            params,
        )
        return {int(row[0]) for row in cur.fetchall()}
