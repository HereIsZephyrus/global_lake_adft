"""Database operations for comparison_run_status table."""
from __future__ import annotations
from typing import Any
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE, year_month_key_sql
_default_table_config = TableConfig.default()







def _ensure_comparison_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id         BIGINT        NOT NULL,
    workflow_version VARCHAR(64)   NOT NULL,
    status           VARCHAR(16)   NOT NULL,
    error_message    TEXT,
    created_at       TIMESTAMPTZ   DEFAULT now(),
    PRIMARY KEY (hylak_id, workflow_version)
);
""").format(table=sql.Identifier(tc.series_table("comparison_run_status")))

def _upsert_comparison_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, workflow_version, status, error_message, created_at
) VALUES (
    %(hylak_id)s, %(workflow_version)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    created_at    = now();
""").format(
        table=sql.Identifier(tc.series_table("comparison_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id", "workflow_version")
        ),
    )

def _fetch_comparison_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id
FROM {{table}}
WHERE {CHUNK_WHERE}
  AND status = 'done'
  AND workflow_version = %(workflow_version)s
""").format(table=sql.Identifier(tc.series_table("comparison_run_status")))

def ensure_comparison_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_comparison_run_status_table_sql(table_config))
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

def fetch_comparison_status_ids_in_range(
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
            _fetch_comparison_status_ids_in_range_sql(table_config),
            params,
        )
        return {int(row[0]) for row in cur.fetchall()}
