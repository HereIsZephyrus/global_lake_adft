"""Unified read interface for PWM extreme results with SQL-side aggregation.

Tables are created by the pwm_extreme batch runner.  This module provides
grid-aggregation queries for global map visualization.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from psycopg import sql as psql

from lakesource.config import Backend, SourceConfig
from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent.parent / "data" / "pwm_extreme"


def _convergence_grid_agg_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT t.hylak_id)                    AS lake_count,
       AVG(CASE WHEN t.converged THEN 1.0 ELSE 0.0 END) AS convergence_rate
FROM   {thresholds} t
JOIN   {lake_info} l ON l.hylak_id = t.hylak_id
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        thresholds=psql.Identifier(tc.series_table("pwm_extreme_thresholds")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _converged_grid_agg_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT t.hylak_id)                    AS lake_count,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.threshold_high) AS median_threshold_high,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.threshold_low)  AS median_threshold_low
FROM   {thresholds} t
JOIN   {lake_info} l ON l.hylak_id = t.hylak_id
WHERE  t.converged IS TRUE
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        thresholds=psql.Identifier(tc.series_table("pwm_extreme_thresholds")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _monthly_threshold_grid_agg_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT t.month,
       FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT t.hylak_id)                    AS lake_count,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.threshold_high) AS median_threshold_high,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.threshold_low)  AS median_threshold_low
FROM   {thresholds} t
JOIN   {lake_info} l ON l.hylak_id = t.hylak_id
WHERE  t.converged IS TRUE
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""").format(
        thresholds=psql.Identifier(tc.series_table("pwm_extreme_thresholds")),
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
    for col in ("lake_count",):
        if col in df.columns:
            df[col] = df[col].astype(int)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Cached %d rows to %s", len(df), cache_path)
    return df


def fetch_pwm_convergence_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"convergence_grid_agg_r{resolution}.parquet"
    return _fetch_and_cache(
        _convergence_grid_agg_sql(config.t),
        {"res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_pwm_converged_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"converged_grid_agg_r{resolution}.parquet"
    return _fetch_and_cache(
        _converged_grid_agg_sql(config.t),
        {"res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_pwm_monthly_threshold_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    cache = (data_dir or _DATA_DIR) / f"monthly_threshold_grid_agg_r{resolution}.parquet"
    df = _fetch_and_cache(
        _monthly_threshold_grid_agg_sql(config.t),
        {"res": resolution},
        cache,
        refresh=refresh,
    )
    if "month" in df.columns:
        df["month"] = df["month"].astype(int)
    return df
