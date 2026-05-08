"""Database operations for interpolation_detect table."""
from __future__ import annotations
from typing import Any
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE, year_month_key_sql
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)







def _ensure_interpolation_detect_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id           INTEGER      PRIMARY KEY,
    n_linear_segments  INTEGER      NOT NULL,
    n_flat_segments    INTEGER      NOT NULL,
    max_linear_len     INTEGER      NOT NULL,
    max_flat_len       INTEGER      NOT NULL,
    collinear_ratio    DOUBLE PRECISION NOT NULL,
    first_linear_ym    INTEGER,
    n_obs              INTEGER      NOT NULL,
    computed_at        TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("interpolation_detect")))

def _upsert_interpolation_detect_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, n_linear_segments, n_flat_segments, max_linear_len,
    max_flat_len, collinear_ratio, first_linear_ym, n_obs, computed_at
) VALUES (
    %(hylak_id)s, %(n_linear_segments)s, %(n_flat_segments)s, %(max_linear_len)s,
    %(max_flat_len)s, %(collinear_ratio)s, %(first_linear_ym)s, %(n_obs)s, now()
)
ON CONFLICT (hylak_id) DO UPDATE SET
    n_linear_segments = EXCLUDED.n_linear_segments,
    n_flat_segments   = EXCLUDED.n_flat_segments,
    max_linear_len    = EXCLUDED.max_linear_len,
    max_flat_len      = EXCLUDED.max_flat_len,
    collinear_ratio   = EXCLUDED.collinear_ratio,
    first_linear_ym   = EXCLUDED.first_linear_ym,
    n_obs             = EXCLUDED.n_obs,
    computed_at       = now();
""").format(table=sql.Identifier(tc.series_table("interpolation_detect")))

def ensure_interpolation_detect_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_interpolation_detect_table_sql(table_config))
    conn.commit()
    log.debug("Ensured interpolation_detect table exists")

def upsert_interpolation_detect(
    conn: psycopg.Connection,
    rows: list[dict[str, Any]],
    *,
    table_config: TableConfig = _default_table_config,
    commit: bool = True,
) -> None:
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_interpolation_detect_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d interpolation_detect row(s)", len(rows))
