"""Unified read interface for EOT results with backend dispatch and parquet cache.

Data is first fetched from the database, then cached to ``data/eot/`` as
parquet files.  Subsequent calls load from cache unless ``refresh=True``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path

import pandas as pd
from psycopg import sql as psql

from lakesource.config import Backend, SourceConfig
from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent / "data" / "eot"


def _cache_path(tail: str, threshold_quantile: float) -> Path:
    q_tag = f"q{threshold_quantile:.4f}"
    return _DATA_DIR / f"eot_results_{tail}_{q_tag}.parquet"


def _eot_results_with_coords_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT r.hylak_id,
       r.tail,
       r.threshold_quantile,
       r.converged,
       r.log_likelihood,
       r.threshold,
       r.n_extremes,
       r.n_observations,
       r.n_frozen_months,
       r.beta0,
       r.beta1,
       r.sin_1,
       r.cos_1,
       r.sigma,
       r.xi,
       r.error_message,
       ST_Y(l.centroid) AS lat,
       ST_X(l.centroid) AS lon
FROM   {eot_results} r
JOIN   {lake_info} l ON l.hylak_id = r.hylak_id
WHERE  r.tail = %(tail)s
  AND  r.threshold_quantile = %(threshold_quantile)s
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
        lake_info=psql.Identifier(tc.series_table("lake_info")),
    )


def _available_quantiles_sql(tc: TableConfig) -> psql.Composed:
    return psql.SQL("""
SELECT DISTINCT tail, threshold_quantile
FROM   {eot_results}
ORDER  BY tail, threshold_quantile
""").format(
        eot_results=psql.Identifier(tc.series_table("eot_results")),
    )


def _fetch_eot_results_postgres(
    config: SourceConfig,
    tail: str,
    threshold_quantile: float,
) -> pd.DataFrame:
    from lakesource.postgres import series_db

    params: dict = {
        "tail": tail,
        "threshold_quantile": Decimal(str(threshold_quantile)),
    }
    sql = _eot_results_with_coords_sql(config.t)
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=colnames)
    return _normalize_eot_df(df)


def _fetch_available_quantiles_postgres(config: SourceConfig) -> pd.DataFrame:
    from lakesource.postgres import series_db

    sql = _available_quantiles_sql(config.t)
    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    return pd.DataFrame(rows, columns=colnames)


def _normalize_eot_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ("hylak_id", "n_extremes", "n_observations", "n_frozen_months"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in (
        "threshold_quantile", "log_likelihood", "threshold",
        "beta0", "beta1", "sin_1", "cos_1", "sigma", "xi",
        "lat", "lon",
    ):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    if "converged" in df.columns:
        df["converged"] = df["converged"].astype(bool)
    if "tail" in df.columns:
        df["tail"] = df["tail"].astype(str)
    return df


def fetch_eot_results_with_coords(
    config: SourceConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    """Fetch EOT results joined with lake coordinates, with parquet cache.

    On first call (or ``refresh=True``), data is fetched from the database
    and cached to ``data/eot/eot_results_{tail}_q{quantile}.parquet``.
    Subsequent calls load from cache.

    Args:
        config: Data source configuration.
        tail: Tail direction ("high" or "low").
        threshold_quantile: Quantile level (e.g. 0.90, 0.95, 0.98).
        refresh: If True, re-fetch from database and overwrite cache.
        data_dir: Override cache directory (default: data/eot/).

    Returns:
        DataFrame with columns:
            hylak_id, tail, threshold_quantile, converged, log_likelihood,
            threshold, n_extremes, n_observations, n_frozen_months,
            beta0, beta1, sin_1, cos_1, sigma, xi, error_message,
            lat, lon.
    """
    cache = (data_dir or _DATA_DIR) / _cache_path(tail, threshold_quantile).name

    if not refresh and cache.exists():
        log.info("Loading EOT results from cache: %s", cache)
        return pd.read_parquet(cache)

    if config.backend == Backend.POSTGRES:
        df = _fetch_eot_results_postgres(config, tail, threshold_quantile)
    else:
        raise NotImplementedError("Parquet backend for EOT results is not yet implemented")

    cache.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache, index=False)
    log.info("Cached EOT results (%d rows) to %s", len(df), cache)
    return df


def fetch_available_quantiles(
    config: SourceConfig,
) -> pd.DataFrame:
    """Fetch distinct (tail, threshold_quantile) combinations from eot_results.

    Returns:
        DataFrame with columns [tail, threshold_quantile].
    """
    if config.backend == Backend.POSTGRES:
        return _fetch_available_quantiles_postgres(config)
    raise NotImplementedError("Parquet backend for EOT quantiles is not yet implemented")
