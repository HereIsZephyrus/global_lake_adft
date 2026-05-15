"""PWM extreme grid aggregation queries registered via grid_query protocol."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from lakesource.grid_cache import cached_or_compute
from lakesource.provider.grid_query import register_grid_query

log = logging.getLogger(__name__)


def _fix_grid_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("cell_lat", "cell_lon"):
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df


class _PWMConvergenceQuery:
    name = "pwm.convergence"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "pwm_extreme" / f"convergence_grid_agg_r{resolution}.parquet"
        return cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT t.hylak_id)                    AS lake_count,
                   AVG(CASE WHEN t.converged THEN 1.0 ELSE 0.0 END) AS convergence_rate
            FROM   pwm_extreme_thresholds t
            JOIN   lake_info l ON l.hylak_id = t.hylak_id
            WHERE  1=1
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ), log=log)

class _PWMConvergedQuery:
    name = "pwm.converged"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "pwm_extreme" / f"converged_grid_agg_r{resolution}.parquet"
        return cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT t.hylak_id)                    AS lake_count,
                   MEDIAN(t.threshold_high)                      AS median_threshold_high,
                   MEDIAN(t.threshold_low)                       AS median_threshold_low
            FROM   pwm_extreme_thresholds t
            JOIN   lake_info l ON l.hylak_id = t.hylak_id
            WHERE  t.converged = true
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ), log=log)

class _PWMMonthlyThresholdQuery:
    name = "pwm.monthly_threshold"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "pwm_extreme" / f"monthly_threshold_grid_agg_r{resolution}.parquet"
        df = cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT t.month,
                   FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT t.hylak_id)                    AS lake_count,
                   MEDIAN(t.threshold_high)                      AS median_threshold_high,
                   MEDIAN(t.threshold_low)                       AS median_threshold_low
            FROM   pwm_extreme_thresholds t
            JOIN   lake_info l ON l.hylak_id = t.hylak_id
            WHERE  t.converged = true
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """)
        ), log=log)
        df["month"] = df["month"].astype(int)
        return df

class _PWMExceedanceQuery:
    name = "pwm.exceedance"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, p_high: float = 0.05, p_low: float = 0.05,
        **kwargs: Any,
    ) -> pd.DataFrame:
        p_tag = f"p{p_high:.4f}"
        cache = cache_dir / "pwm_extreme" / f"exceedance_grid_agg_{p_tag}_r{resolution}.parquet"
        return cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            WITH exceedance AS (
                SELECT hylak_id,
                       SUM(CASE WHEN extreme_label = 'extreme_high' THEN 1 ELSE 0 END) AS high_count,
                       SUM(CASE WHEN extreme_label = 'extreme_low'  THEN 1 ELSE 0 END) AS low_count
                FROM   pwm_extreme_labels
                GROUP BY hylak_id
            )
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT e.hylak_id)                 AS lake_count,
                   AVG(e.high_count)                           AS mean_high_exceedance,
                   AVG(e.low_count)                            AS mean_low_exceedance,
                   AVG(e.high_count + e.low_count)             AS mean_all_exceedance,
                   MEDIAN(e.high_count)                        AS median_high_exceedance,
                   MEDIAN(e.low_count)                         AS median_low_exceedance,
                   MEDIAN(e.high_count + e.low_count)          AS median_all_exceedance
            FROM   exceedance e
            JOIN   lake_info l ON l.hylak_id = e.hylak_id
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ), log=log)

class _PWMMonthlyExceedanceQuery:
    name = "pwm.monthly_exceedance"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, p_high: float = 0.05, p_low: float = 0.05,
        **kwargs: Any,
    ) -> pd.DataFrame:
        p_tag = f"p{p_high:.4f}"
        cache = cache_dir / "pwm_extreme" / f"monthly_exceedance_grid_agg_{p_tag}_r{resolution}.parquet"
        df = cached_or_compute(cache, refresh=refresh, compute_fn=lambda: _fix_grid_dtypes(
            client.query_df(f"""
            WITH exceedance AS (
                SELECT hylak_id, month,
                       SUM(CASE WHEN extreme_label = 'extreme_high' THEN 1 ELSE 0 END) AS high_count,
                       SUM(CASE WHEN extreme_label = 'extreme_low'  THEN 1 ELSE 0 END) AS low_count
                FROM   pwm_extreme_labels
                GROUP BY hylak_id, month
            )
            SELECT e.month,
                   FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT e.hylak_id)                AS lake_count,
                   AVG(e.high_count)                          AS high_exceedance_rate,
                   AVG(e.low_count)                           AS low_exceedance_rate
            FROM   exceedance e
            JOIN   lake_info l ON l.hylak_id = e.hylak_id
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """)
        ), log=log)
        df["month"] = df["month"].astype(int)
        return df

register_grid_query(_PWMConvergenceQuery())
register_grid_query(_PWMConvergedQuery())
register_grid_query(_PWMMonthlyThresholdQuery())
register_grid_query(_PWMExceedanceQuery())
register_grid_query(_PWMMonthlyExceedanceQuery())
