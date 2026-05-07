"""Database operations for lake_area reads."""

from __future__ import annotations

import logging

import pandas as pd
import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig
from lakesource.postgres.sql_templates import year_month_extract_sql

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _fetch_lake_area_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym}
       water_area
FROM {table}
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("lake_area")),
        ym=year_month_extract_sql(),
    )


def _fetch_lake_area_limited_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym}
       water_area
FROM {table}
WHERE hylak_id < %(limit_id)s
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("lake_area")),
        ym=year_month_extract_sql(),
    )


def _fetch_lake_area_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT la.hylak_id,
       {ym}
       la.water_area
FROM {lake_area} la
WHERE la.hylak_id >= %(chunk_start)s::bigint AND la.hylak_id < %(chunk_end)s::bigint
ORDER BY la.hylak_id, la.year_month
""").format(
        lake_area=sql.Identifier(tc.series_table("lake_area")),
        ym=year_month_extract_sql("la"),
    )


def _fetch_lake_area_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id,
       {ym}
       water_area
FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
ORDER BY hylak_id, year_month
""").format(
        table=sql.Identifier(tc.series_table("lake_area")),
        ym=year_month_extract_sql(),
    )


def _fetch_af_nearest_high_topo_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, nearest_id, topo_level
FROM {table}
WHERE topo_level > 8 AND nearest_id IS NOT NULL
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("af_nearest")))


def _fetch_impact_pairs_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT a.hylak_id, a.nearest_id, a.topo_level
FROM {af_nearest} a
LEFT JOIN {anomalies} ax ON a.hylak_id = ax.hylak_id
LEFT JOIN {anomalies} an ON a.nearest_id = an.hylak_id
WHERE a.topo_level > 8
  AND a.nearest_id IS NOT NULL
  AND ax.hylak_id IS NULL
  AND an.hylak_id IS NULL
ORDER BY a.hylak_id
""").format(
        af_nearest=sql.Identifier(tc.series_table("af_nearest")),
        anomalies=sql.Identifier(tc.series_table("area_anomalies")),
    )


def fetch_lake_area(
    conn: psycopg.Connection,
    limit_id: int | None = None,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, pd.DataFrame]:
    """Fetch all lake_area rows and split by hylak_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        limit_id: If given, only rows with id < limit_id are returned (for testing).
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].
    """
    if limit_id is None:
        query = _fetch_lake_area_sql(table_config)
        params = None
    else:
        query = _fetch_lake_area_limited_sql(table_config)
        params = {"limit_id": limit_id}

    with conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    result = {
        int(hylak_id): group.drop(columns="hylak_id").reset_index(drop=True)
        for hylak_id, group in df.groupby("hylak_id")
    }
    log.info("Fetched lake_area: %d rows, %d lakes", len(df), len(result))
    return result


def fetch_lake_area_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, pd.DataFrame]:
    """Fetch lake_area rows for a hylak_id range [chunk_start, chunk_end) and split by hylak_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].
    """
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_lake_area_chunk_sql(table_config), params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return {}
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    result = {
        int(hylak_id): group.drop(columns="hylak_id").reset_index(drop=True)
        for hylak_id, group in df.groupby("hylak_id")
    }
    log.debug(
        "Fetched lake_area chunk [%d, %d): %d rows, %d lakes",
        chunk_start,
        chunk_end,
        len(df),
        len(result),
    )
    return result


def fetch_af_nearest_high_topo(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> list[dict]:
    """Fetch af_nearest rows with topo_level > 8 and non-null nearest_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        table_config: Table name configuration.

    Returns:
        List of dicts with keys hylak_id, nearest_id, topo_level.
    """
    with conn.cursor() as cur:
        cur.execute(_fetch_af_nearest_high_topo_sql(table_config))
        rows = cur.fetchall()
    result = [
        {"hylak_id": int(r[0]), "nearest_id": int(r[1]), "topo_level": int(r[2])}
        for r in rows
    ]
    log.info("Fetched af_nearest (topo_level>8): %d pairs", len(result))
    return result


def fetch_impact_pairs(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> list[dict]:
    """Fetch quality-filtered af_nearest pairs (topo_level > 8, no anomalous lakes).

    Excludes pairs where either lake appears in area_anomalies, pushing the
    filter down to SQL instead of loading all anomaly IDs into Python.

    Args:
        conn: Open connection to SERIES_DB.
        table_config: Table name configuration.

    Returns:
        List of dicts with keys hylak_id, nearest_id, topo_level.
    """
    with conn.cursor() as cur:
        cur.execute(_fetch_impact_pairs_sql(table_config))
        rows = cur.fetchall()
    result = [
        {"hylak_id": int(r[0]), "nearest_id": int(r[1]), "topo_level": int(r[2])}
        for r in rows
    ]
    log.info("Fetched impact pairs (topo_level>8, quality-filtered): %d pairs", len(result))
    return result


def fetch_lake_area_by_ids(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, pd.DataFrame]:
    """Fetch lake_area rows for the given hylak_id set and split by hylak_id.

    Args:
        conn: An open psycopg connection to SERIES_DB.
        id_list: List of hylak_id values to fetch.
        table_config: Table name configuration.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].
    """
    if not id_list:
        return {}
    params = {"id_list": id_list}
    with conn.cursor() as cur:
        cur.execute(_fetch_lake_area_by_ids_sql(table_config), params)
        rows = cur.fetchall()
        colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    if df.empty:
        return {}
    df["year"] = df["year"].astype(int)
    df["month"] = df["month"].astype(int)
    df["water_area"] = df["water_area"].astype(float)

    result = {
        int(hylak_id): group.drop(columns="hylak_id").reset_index(drop=True)
        for hylak_id, group in df.groupby("hylak_id")
    }
    log.debug(
        "Fetched lake_area by ids: %d rows, %d lakes",
        len(df),
        len(result),
    )
    return result
