"""Database operations for quality_run_status table."""
from __future__ import annotations
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)





RUN_STATUS_DONE = "done"

RUN_STATUS_ERROR = "error"

def _ensure_quality_run_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id         INTEGER      NOT NULL,
    chunk_start      INTEGER,
    chunk_end        INTEGER,
    status           VARCHAR(16)  NOT NULL,
    error_message    TEXT,
    computed_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("quality_run_status")))

def _upsert_quality_run_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (hylak_id, chunk_start, chunk_end, status, error_message, computed_at)
VALUES (%(hylak_id)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now())
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(
        table=sql.Identifier(tc.series_table("quality_run_status")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )

def ensure_quality_run_status_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_quality_run_status_table_sql(table_config))
    conn.commit()
    log.debug("Ensured quality_run_status table exists")

def upsert_quality_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_quality_run_status_sql(table_config), rows)
    conn.commit()
    log.info("Upserted %d quality_run_status row(s)", len(rows))

def make_quality_run_status_row(
    hylak_id: int,
    status: str,
    *,
    chunk_start: int = 0,
    chunk_end: int = 0,
    error_message: str | None = None,
) -> dict:
    return {
        "hylak_id": hylak_id,
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
        "status": status,
        "error_message": error_message,
    }
