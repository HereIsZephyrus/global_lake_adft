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


def _crossent_threshold_sql_pg(p: float, direction: str) -> str:
    u = 1.0 - p if direction == "high" else p
    ln_arg = 1.0 - u
    return f"""t.mean_area * (
               (t.epsilon - (1 - t.epsilon) * LN({ln_arg}))
               * EXP(-(t.lambda_0
                     + t.lambda_1 * {u}
                     + t.lambda_2 * {u} * {u}
                     + t.lambda_3 * {u} * {u} * {u}
                     + t.lambda_4 * {u} * {u} * {u} * {u}))
           )"""


def _exceedance_grid_agg_sql(tc: TableConfig, p_high: float, p_low: float) -> psql.Composed:
    th_high = _crossent_threshold_sql_pg(p_high, "high")
    th_low = _crossent_threshold_sql_pg(p_low, "low")
    return psql.SQL("""
WITH deduped_area AS (
    SELECT DISTINCT hylak_id, year_month, water_area
    FROM   {lake_area}
),
quantile_thresholds AS (
    SELECT t.hylak_id, t.month,
           """ + psql.SQL(th_high).sql.decode() + """ AS threshold_high,
           """ + psql.SQL(th_low).sql.decode() + """ AS threshold_low
    FROM   {thresholds} t
    WHERE  t.converged IS TRUE
),
exceedance AS (
    SELECT la.hylak_id,
           SUM(CASE WHEN la.water_area >= qt.threshold_high THEN 1 ELSE 0 END) AS high_count,
           SUM(CASE WHEN la.water_area <= qt.threshold_low  THEN 1 ELSE 0 END) AS low_count
    FROM   deduped_area la
    JOIN   quantile_thresholds qt
      ON   qt.hylak_id = la.hylak_id
      AND  qt.month = EXTRACT(MONTH FROM la.year_month)
    GROUP BY la.hylak_id
)
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT e.hylak_id)                  AS lake_count,
       AVG(e.high_count)                            AS mean_high_exceedance,
       AVG(e.low_count)                             AS mean_low_exceedance,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.high_count) AS median_high_exceedance,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.low_count)  AS median_low_exceedance
FROM   exceedance e
JOIN   {lake_info} l ON l.hylak_id = e.hylak_id
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        thresholds=psql.Identifier(tc.series_table("pwm_extreme_thresholds")),
        lake_area=psql.Identifier(tc.series_table("lake_area")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _monthly_exceedance_grid_agg_sql(tc: TableConfig, p_high: float, p_low: float) -> psql.Composed:
    th_high = _crossent_threshold_sql_pg(p_high, "high")
    th_low = _crossent_threshold_sql_pg(p_low, "low")
    return psql.SQL("""
WITH deduped_area AS (
    SELECT DISTINCT hylak_id, year_month, water_area
    FROM   {lake_area}
),
quantile_thresholds AS (
    SELECT t.hylak_id, t.month,
           """ + psql.SQL(th_high).sql.decode() + """ AS threshold_high,
           """ + psql.SQL(th_low).sql.decode() + """ AS threshold_low
    FROM   {thresholds} t
    WHERE  t.converged IS TRUE
)
SELECT qt.month,
       FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT la.hylak_id)                  AS lake_count,
       AVG(CASE WHEN la.water_area >= qt.threshold_high THEN 1.0 ELSE 0.0 END) AS high_exceedance_rate,
       AVG(CASE WHEN la.water_area <= qt.threshold_low  THEN 1.0 ELSE 0.0 END) AS low_exceedance_rate
FROM   deduped_area la
JOIN   quantile_thresholds qt
  ON   qt.hylak_id = la.hylak_id
  AND  qt.month = EXTRACT(MONTH FROM la.year_month)
JOIN   {lake_info} l ON l.hylak_id = la.hylak_id
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""").format(
        thresholds=psql.Identifier(tc.series_table("pwm_extreme_thresholds")),
        lake_area=psql.Identifier(tc.series_table("lake_area")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def fetch_pwm_exceedance_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    p_high: float = 0.05,
    p_low: float = 0.05,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    p_tag = f"p{p_high:.4f}"
    cache = (data_dir or _DATA_DIR) / f"exceedance_grid_agg_{p_tag}_r{resolution}.parquet"
    return _fetch_and_cache(
        _exceedance_grid_agg_sql(config.t, p_high, p_low),
        {"res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_pwm_monthly_exceedance_grid_agg(
    config: SourceConfig,
    resolution: float = 0.5,
    *,
    p_high: float = 0.05,
    p_low: float = 0.05,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    p_tag = f"p{p_high:.4f}"
    cache = (data_dir or _DATA_DIR) / f"monthly_exceedance_grid_agg_{p_tag}_r{resolution}.parquet"
    df = _fetch_and_cache(
        _monthly_exceedance_grid_agg_sql(config.t, p_high, p_low),
        {"res": resolution},
        cache,
        refresh=refresh,
    )
    if "month" in df.columns:
        df["month"] = df["month"].astype(int)
    return df
