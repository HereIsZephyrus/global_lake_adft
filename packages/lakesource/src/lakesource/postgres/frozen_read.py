"""Database operations for frozen anomaly month reads."""
from __future__ import annotations
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import CHUNK_WHERE, year_month_key_sql
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)







def _fetch_frozen_year_months_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym_key}
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
  AND anomaly_type = 'frozen'
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("anomaly")),
        ym_key=year_month_key_sql(),
    )

def _fetch_frozen_year_months_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym_key}
FROM {table}
WHERE {chunk}
  AND anomaly_type = 'frozen'
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("anomaly")),
        chunk=sql.SQL(CHUNK_WHERE),
    )

def fetch_frozen_year_months_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, set[int]]:
    """Fetch YYYYMM keys flagged as frozen for the given hylak_ids."""
    if not id_list:
        return {}
    params = {"id_list": id_list}
    with conn.cursor() as cur:
        cur.execute(_fetch_frozen_year_months_by_ids_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, set[int]] = {int(hid): set() for hid in id_list}
    for hylak_id, year_month_key in rows:
        result[int(hylak_id)].add(int(year_month_key))
    log.debug("Fetched frozen anomaly months for %d lake(s)", len(result))
    return result

def fetch_frozen_year_months_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, set[int]]:
    """Fetch YYYYMM keys flagged as frozen for a hylak_id range."""
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_frozen_year_months_chunk_sql(table_config), params)
        rows = cur.fetchall()
    result: dict[int, set[int]] = {}
    for hylak_id, year_month_key in rows:
        hid = int(hylak_id)
        if hid not in result:
            result[hid] = set()
        result[hid].add(int(year_month_key))
    log.debug(
        "Fetched frozen months for chunk [%d, %d): %d lake(s) with frozen months",
        chunk_start,
        chunk_end,
        len(result),
    )
    return result
