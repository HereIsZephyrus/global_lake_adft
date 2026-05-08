"""Database operations for lake_info metadata reads."""
from __future__ import annotations
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)







def _fetch_seasonal_amplitude_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id, annual_means_std, mean_area
FROM {{table}}
WHERE {CHUNK_WHERE}
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))

def _fetch_linear_trend_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, linear_trend_of_stl_trend_per_period
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))

def _fetch_max_lake_info_hylak_id_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT MAX(hylak_id)
FROM {table}
""").format(table=sql.Identifier(tc.series_table("lake_area")))

def _count_source_hylak_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT COUNT(*)
FROM {{table}}
WHERE {CHUNK_WHERE}
""").format(table=sql.Identifier(tc.series_table("lake_info")))

def _fetch_source_hylak_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL(f"""
SELECT hylak_id
FROM {{table}}
WHERE {CHUNK_WHERE}
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))

def fetch_seasonal_amplitude_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float | None]:
    """Fetch CV from lake_info: annual_means_std / mean_area."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_seasonal_amplitude_chunk_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, float | None] = {}
    for r in rows:
        hylak_id = int(r[0])
        annual_means_std = float(r[1]) if r[1] is not None else None
        mean_area = float(r[2]) if r[2] is not None else None
        if annual_means_std is not None and mean_area is not None and mean_area > 0:
            result[hylak_id] = annual_means_std / mean_area
        else:
            result[hylak_id] = None
    return result

def fetch_linear_trend_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float | None]:
    """Fetch linear trend from lake_info for the given hylak_ids."""
    if not id_list:
        return {}
    params = {"id_list": id_list}
    with conn.cursor() as cur:
        cur.execute(_fetch_linear_trend_by_ids_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, float | None] = {}
    for r in rows:
        hylak_id = int(r[0])
        trend = float(r[1]) if r[1] is not None else None
        result[hylak_id] = trend
    for hid in id_list:
        if hid not in result:
            result[hid] = None
    log.debug("Fetched linear_trend for %d lake(s)", len(result))
    return result

def fetch_max_lake_info_hylak_id(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> int | None:
    """Return the maximum hylak_id present in lake_info."""
    with conn.cursor() as cur:
        cur.execute(_fetch_max_lake_info_hylak_id_sql(table_config))
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None

def count_source_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    """Count source lake ids from lake_info in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_count_source_hylak_ids_in_range_sql(table_config), params)
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def fetch_source_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch source lake ids from lake_info in a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_source_hylak_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}
