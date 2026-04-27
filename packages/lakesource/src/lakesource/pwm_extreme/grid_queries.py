"""PWM extreme grid aggregation queries registered via grid_query protocol."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from lakesource.provider.grid_query import register_grid_query

log = logging.getLogger(__name__)


def _fix_grid_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("cell_lat", "cell_lon"):
        if col in df.columns:
            df[col] = df[col].astype(float)
    return df


def _cached_or_compute(
    cache_path: Path, refresh: bool, compute_fn
) -> pd.DataFrame:
    if not refresh and cache_path.exists():
        log.info("Loading from cache: %s", cache_path)
        return pd.read_parquet(cache_path)
    df = compute_fn()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Cached %d rows to %s", len(df), cache_path)
    return df


def _crossent_threshold_sql(p: float, direction: str) -> str:
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


class _PWMConvergenceQuery:
    name = "pwm.convergence"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "pwm_extreme" / f"convergence_grid_agg_r{resolution}.parquet"
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
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
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_convergence_grid_agg
        return fetch_pwm_convergence_grid_agg(config, resolution, refresh=refresh)


class _PWMConvergedQuery:
    name = "pwm.converged"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "pwm_extreme" / f"converged_grid_agg_r{resolution}.parquet"
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
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
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_converged_grid_agg
        return fetch_pwm_converged_grid_agg(config, resolution, refresh=refresh)


class _PWMMonthlyThresholdQuery:
    name = "pwm.monthly_threshold"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "pwm_extreme" / f"monthly_threshold_grid_agg_r{resolution}.parquet"
        df = _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
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
        ))
        df["month"] = df["month"].astype(int)
        return df

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_monthly_threshold_grid_agg
        return fetch_pwm_monthly_threshold_grid_agg(config, resolution, refresh=refresh)


class _PWMExceedanceQuery:
    name = "pwm.exceedance"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, p_high: float = 0.05, p_low: float = 0.05,
        **kwargs: Any,
    ) -> pd.DataFrame:
        p_tag = f"p{p_high:.4f}"
        cache = cache_dir / "pwm_extreme" / f"exceedance_grid_agg_{p_tag}_r{resolution}.parquet"
        th_high = _crossent_threshold_sql(p_high, "high")
        th_low = _crossent_threshold_sql(p_low, "low")
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            WITH deduped_area AS (
                SELECT DISTINCT hylak_id, year_month, water_area
                FROM lake_area
            ),
            quantile_thresholds AS (
                SELECT t.hylak_id, t.month,
                       {th_high} AS threshold_high,
                       {th_low}  AS threshold_low
                FROM   pwm_extreme_thresholds t
                WHERE  t.converged = true
            ),
            exceedance AS (
                SELECT la.hylak_id,
                       SUM(CASE WHEN la.water_area >= qt.threshold_high THEN 1 ELSE 0 END) AS high_count,
                       SUM(CASE WHEN la.water_area <= qt.threshold_low  THEN 1 ELSE 0 END) AS low_count
                FROM   deduped_area la
                JOIN   quantile_thresholds qt
                  ON   qt.hylak_id = la.hylak_id
                  AND  qt.month = MONTH(la.year_month)
                GROUP BY la.hylak_id
            )
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT e.hylak_id)                 AS lake_count,
                   AVG(e.high_count)                           AS mean_high_exceedance,
                   AVG(e.low_count)                            AS mean_low_exceedance,
                   MEDIAN(e.high_count)                        AS median_high_exceedance,
                   MEDIAN(e.low_count)                         AS median_low_exceedance
            FROM   exceedance e
            JOIN   lake_info l ON l.hylak_id = e.hylak_id
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, p_high: float = 0.05, p_low: float = 0.05,
        **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_exceedance_grid_agg
        return fetch_pwm_exceedance_grid_agg(
            config, resolution, p_high=p_high, p_low=p_low, refresh=refresh
        )


class _PWMMonthlyExceedanceQuery:
    name = "pwm.monthly_exceedance"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, p_high: float = 0.05, p_low: float = 0.05,
        **kwargs: Any,
    ) -> pd.DataFrame:
        p_tag = f"p{p_high:.4f}"
        cache = cache_dir / "pwm_extreme" / f"monthly_exceedance_grid_agg_{p_tag}_r{resolution}.parquet"
        th_high = _crossent_threshold_sql(p_high, "high")
        th_low = _crossent_threshold_sql(p_low, "low")
        df = _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            WITH deduped_area AS (
                SELECT DISTINCT hylak_id, year_month, water_area
                FROM lake_area
            ),
            quantile_thresholds AS (
                SELECT t.hylak_id, t.month,
                       {th_high} AS threshold_high,
                       {th_low}  AS threshold_low
                FROM   pwm_extreme_thresholds t
                WHERE  t.converged = true
            )
            SELECT qt.month,
                   FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT la.hylak_id)                AS lake_count,
                   AVG(CASE WHEN la.water_area >= qt.threshold_high THEN 1.0 ELSE 0.0 END) AS high_exceedance_rate,
                   AVG(CASE WHEN la.water_area <= qt.threshold_low  THEN 1.0 ELSE 0.0 END) AS low_exceedance_rate
            FROM   deduped_area la
            JOIN   quantile_thresholds qt
              ON   qt.hylak_id = la.hylak_id
              AND  qt.month = MONTH(la.year_month)
            JOIN   lake_info l ON l.hylak_id = la.hylak_id
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """)
        ))
        df["month"] = df["month"].astype(int)
        return df

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, p_high: float = 0.05, p_low: float = 0.05,
        **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_monthly_exceedance_grid_agg
        return fetch_pwm_monthly_exceedance_grid_agg(
            config, resolution, p_high=p_high, p_low=p_low, refresh=refresh
        )


register_grid_query(_PWMConvergenceQuery())
register_grid_query(_PWMConvergedQuery())
register_grid_query(_PWMMonthlyThresholdQuery())
register_grid_query(_PWMExceedanceQuery())
register_grid_query(_PWMMonthlyExceedanceQuery())