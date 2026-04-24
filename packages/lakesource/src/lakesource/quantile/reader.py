"""Unified read interface for quantile-based identification data with SQL-side aggregation.

All global-map queries perform aggregation in PostgreSQL, returning only
~37k grid cells instead of millions of raw rows.  Results are cached as
parquet in ``data/quantile/``.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg import sql as psql

from lakesource.config import Backend, SourceConfig
from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data" / "quantile"


def _extremes_grid_agg_sql(tc: TableConfig, resolution: float) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT e.hylak_id)                    AS lake_count,
       COUNT(*)                                       AS event_count
FROM   {extremes} e
JOIN   {lake_info} l ON l.hylak_id = e.hylak_id
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        extremes=psql.Identifier(tc.series_table("quantile_extremes")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _extremes_by_type_grid_agg_sql(tc: TableConfig, resolution: float) -> psql.Composed:
    return psql.SQL("""
SELECT e.event_type,
       FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT e.hylak_id)                    AS lake_count,
       COUNT(*)                                       AS event_count
FROM   {extremes} e
JOIN   {lake_info} l ON l.hylak_id = e.hylak_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""").format(
        extremes=psql.Identifier(tc.series_table("quantile_extremes")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _transitions_grid_agg_sql(tc: TableConfig, resolution: float) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT t.hylak_id)                    AS lake_count,
       COUNT(*)                                       AS event_count
FROM   {transitions} t
JOIN   {lake_info} l ON l.hylak_id = t.hylak_id
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        transitions=psql.Identifier(tc.series_table("quantile_abrupt_transitions")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _transitions_by_type_grid_agg_sql(tc: TableConfig, resolution: float) -> psql.Composed:
    return psql.SQL("""
SELECT t.transition_type,
       FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT t.hylak_id)                    AS lake_count,
       COUNT(*)                                       AS event_count
FROM   {transitions} t
JOIN   {lake_info} l ON l.hylak_id = t.hylak_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""").format(
        transitions=psql.Identifier(tc.series_table("quantile_abrupt_transitions")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _fetch_and_cache(
    sql: psql.Composed,
    params: dict,
    cache_path: Path,
    *,
    refresh: bool = False,
) -> pd.DataFrame:
    if not refresh and cache_path.exists():
        log.info("Loading from cache: %s", cache_path)
        return pd.read_parquet(cache_path)

    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    for col in ("cell_lat", "cell_lon"):
        if col in df.columns:
            df[col] = df[col].astype(float)
    for col in ("lake_count", "event_count"):
        if col in df.columns:
            df[col] = df[col].astype(int)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Cached %d rows to %s", len(df), cache_path)
    return df


def fetch_extremes_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"extremes_grid_agg_r{resolution}.parquet"
    return _fetch_and_cache(
        _extremes_grid_agg_sql(config.t, resolution),
        {"res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_extremes_by_type_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"extremes_by_type_grid_agg_r{resolution}.parquet"
    return _fetch_and_cache(
        _extremes_by_type_grid_agg_sql(config.t, resolution),
        {"res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_transitions_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"transitions_grid_agg_r{resolution}.parquet"
    return _fetch_and_cache(
        _transitions_grid_agg_sql(config.t, resolution),
        {"res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_transitions_by_type_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"transitions_by_type_grid_agg_r{resolution}.parquet"
    return _fetch_and_cache(
        _transitions_by_type_grid_agg_sql(config.t, resolution),
        {"res": resolution},
        cache,
        refresh=refresh,
    )
