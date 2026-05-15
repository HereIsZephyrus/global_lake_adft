"""EOT grid aggregation queries registered via grid_query protocol."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from lakesource.grid_cache import cached_or_compute
from lakesource.grid_dtypes import coerce_grid_columns
from lakesource.provider.grid_query import register_grid_query

log = logging.getLogger(__name__)


def _fix_grid_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    return coerce_grid_columns(
        df,
        int_columns=("lake_count",),
        float_columns=(
            "convergence_rate",
            "median_xi",
            "median_sigma",
            "mean_extremes_freq",
            "median_extremes_freq",
            "median_threshold",
            "mean_all_extremes_freq",
            "median_all_extremes_freq",
        ),
    )


class _EOTConvergenceQuery:
    name = "eot.convergence"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, tail: str = "high", threshold_quantile: float = 0.95,
        **kwargs: Any,
    ) -> pd.DataFrame:
        q_tag = f"{tail}_q{threshold_quantile:.4f}"
        cache = cache_dir / "eot" / f"eot_convergence_{q_tag}_r{resolution}.parquet"
        return cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT r.hylak_id)                    AS lake_count,
                   AVG(CASE WHEN r.converged THEN 1.0 ELSE 0.0 END) AS convergence_rate
            FROM   eot_results r
            JOIN   lake_info l ON l.hylak_id = r.hylak_id
            WHERE  r.tail = '{tail}'
              AND  r.threshold_quantile = '{threshold_quantile}'
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ), log=log)


class _EOTConvergedQuery:
    name = "eot.converged"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, tail: str = "high", threshold_quantile: float = 0.95,
        **kwargs: Any,
    ) -> pd.DataFrame:
        q_tag = f"{tail}_q{threshold_quantile:.4f}"
        cache = cache_dir / "eot" / f"eot_converged_{q_tag}_r{resolution}.parquet"
        return cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT r.hylak_id)                    AS lake_count,
                   MEDIAN(r.xi)                                  AS median_xi,
                   MEDIAN(r.sigma)                               AS median_sigma,
                   AVG(r.n_extremes::float / NULLIF(r.n_observations, 0)) AS mean_extremes_freq,
                   MEDIAN(r.n_extremes::float / NULLIF(r.n_observations, 0)) AS median_extremes_freq,
                   MEDIAN(r.threshold)                           AS median_threshold
            FROM   eot_results r
            JOIN   lake_info l ON l.hylak_id = r.hylak_id
            WHERE  r.tail = '{tail}'
              AND  r.threshold_quantile = '{threshold_quantile}'
              AND  r.converged IS TRUE
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ), log=log)


class _EOTConvergedAllQuery:
    name = "eot.converged_all"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, threshold_quantile: float = 0.95,
        **kwargs: Any,
    ) -> pd.DataFrame:
        q_tag = f"q{threshold_quantile:.4f}"
        cache = cache_dir / "eot" / f"eot_converged_all_{q_tag}_r{resolution}.parquet"
        return cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            WITH paired AS (
                SELECT hi.hylak_id,
                       (hi.n_extremes::float / NULLIF(hi.n_observations, 0))
                         + (lo.n_extremes::float / NULLIF(lo.n_observations, 0)) AS all_extremes_freq
                FROM   eot_results hi
                JOIN   eot_results lo
                  ON   lo.hylak_id = hi.hylak_id
                 AND   lo.tail = 'low'
                 AND   lo.threshold_quantile = '{threshold_quantile}'
                 AND   lo.converged IS TRUE
                WHERE  hi.tail = 'high'
                  AND  hi.threshold_quantile = '{threshold_quantile}'
                  AND  hi.converged IS TRUE
            )
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT p.hylak_id)                 AS lake_count,
                   AVG(p.all_extremes_freq)                   AS mean_all_extremes_freq,
                   MEDIAN(p.all_extremes_freq)                AS median_all_extremes_freq
            FROM   paired p
            JOIN   lake_info l ON l.hylak_id = p.hylak_id
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ), log=log)


register_grid_query(_EOTConvergenceQuery())
register_grid_query(_EOTConvergedQuery())
register_grid_query(_EOTConvergedAllQuery())
