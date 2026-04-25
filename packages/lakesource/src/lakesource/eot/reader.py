"""Unified read interface for EOT results with SQL-side aggregation and parquet cache.

Global-map queries aggregate in PostgreSQL, returning only ~37k grid cells.
Results are cached to ``data/eot/`` as parquet files.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg import sql as psql

from lakesource.config import Backend, SourceConfig
from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent.parent / "data" / "eot"


def _available_quantiles_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT DISTINCT tail, threshold_quantile
FROM   {eot_results}
ORDER  BY tail, threshold_quantile
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
    )


def _eot_grid_agg_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT r.hylak_id)                    AS lake_count,
       AVG(CASE WHEN r.converged THEN 1.0 ELSE 0.0 END) AS convergence_rate,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.xi)   AS median_xi,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.sigma) AS median_sigma,
       AVG(r.n_extremes::float / NULLIF(r.n_observations, 0)) AS mean_extremes_freq,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.threshold) AS median_threshold
FROM   {eot_results} r
JOIN   {lake_info} l ON l.hylak_id = r.hylak_id
WHERE  r.tail = %(tail)s
  AND  r.threshold_quantile = %(threshold_quantile)s
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _eot_convergence_grid_agg_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT r.hylak_id)                    AS lake_count,
       AVG(CASE WHEN r.converged THEN 1.0 ELSE 0.0 END) AS convergence_rate
FROM   {eot_results} r
JOIN   {lake_info} l ON l.hylak_id = r.hylak_id
WHERE  r.tail = %(tail)s
  AND  r.threshold_quantile = %(threshold_quantile)s
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _eot_converged_grid_agg_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT FLOOR(ST_Y(l.centroid) / %(res)s) * %(res)s AS cell_lat,
       FLOOR(ST_X(l.centroid) / %(res)s) * %(res)s AS cell_lon,
       COUNT(DISTINCT r.hylak_id)                    AS lake_count,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.xi)   AS median_xi,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.sigma) AS median_sigma,
       AVG(r.n_extremes::float / NULLIF(r.n_observations, 0)) AS mean_extremes_freq,
       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.threshold) AS median_threshold
FROM   {eot_results} r
JOIN   {lake_info} l ON l.hylak_id = r.hylak_id
WHERE  r.tail = %(tail)s
  AND  r.threshold_quantile = %(threshold_quantile)s
  AND  r.converged IS TRUE
GROUP BY 1, 2
ORDER BY 1, 2
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
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
    for col in ("convergence_rate", "median_xi", "median_sigma", "mean_extremes_freq", "median_threshold"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Cached %d rows to %s", len(df), cache_path)
    return df


def fetch_available_quantiles(config: SourceConfig) -> pd.DataFrame:
    if config.backend != Backend.POSTGRES:
        raise NotImplementedError("Parquet backend for EOT quantiles is not yet implemented")
    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(_available_quantiles_sql(config.t))
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=colnames)


def fetch_eot_convergence_grid_agg(
    config: SourceConfig,
    tail: str,
    threshold_quantile: float,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    q_tag = f"q{threshold_quantile:.4f}"
    cache = (data_dir or _DATA_DIR) / f"eot_convergence_{tail}_{q_tag}_r{resolution}.parquet"
    return _fetch_and_cache(
        _eot_convergence_grid_agg_sql(config.t),
        {"tail": tail, "threshold_quantile": Decimal(str(threshold_quantile)), "res": resolution},
        cache,
        refresh=refresh,
    )


def fetch_eot_converged_grid_agg(
    config: SourceConfig,
    tail: str,
    threshold_quantile: float,
    resolution: float = 0.5,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    q_tag = f"q{threshold_quantile:.4f}"
    cache = (data_dir or _DATA_DIR) / f"eot_converged_{tail}_{q_tag}_r{resolution}.parquet"
    return _fetch_and_cache(
        _eot_converged_grid_agg_sql(config.t),
        {"tail": tail, "threshold_quantile": Decimal(str(threshold_quantile)), "res": resolution},
        cache,
        refresh=refresh,
    )
