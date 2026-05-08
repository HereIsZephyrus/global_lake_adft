"""Cross-table queries for area_anomalies and area_quality lookups."""
from __future__ import annotations
from typing import Any
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE, year_month_key_sql
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)







def _fetch_anomaly_hylak_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT DISTINCT hylak_id
FROM {table}
""").format(table=sql.Identifier(tc.series_table("area_anomalies")))

def _fetch_quality_done_hylak_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT DISTINCT la.hylak_id
FROM {{lake_area}} la
JOIN {{area_quality}} aq ON aq.hylak_id = la.hylak_id
WHERE {CHUNK_WHERE}
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
    )

def _fetch_area_quality_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT DISTINCT la.hylak_id
FROM {{lake_area}} la
JOIN {{area_quality}} aq ON aq.hylak_id = la.hylak_id
WHERE {CHUNK_WHERE}
ORDER BY la.hylak_id
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
    )

def _count_area_quality_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT COUNT(DISTINCT la.hylak_id)
FROM {{lake_area}} la
JOIN {{area_quality}} aq ON aq.hylak_id = la.hylak_id
WHERE {CHUNK_WHERE}
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
    )

def fetch_anomaly_hylak_ids(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch all hylak_id values from area_anomalies."""
    with conn.cursor() as cur:
        cur.execute(_fetch_anomaly_hylak_ids_sql(table_config))
        rows = cur.fetchall()
    result = {int(r[0]) for r in rows}
    log.info("Fetched anomaly hylak_ids: %d lakes", len(result))
    return result

def fetch_quality_done_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch all quality-processed lake ids in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_quality_done_hylak_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}
