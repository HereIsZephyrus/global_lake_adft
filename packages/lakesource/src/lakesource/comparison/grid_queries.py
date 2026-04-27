"""Comparison grid aggregation queries registered via grid_query protocol.

Provides Quantile vs PWM exceedance rate comparison for sampled lakes.
"""

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
    for col in ("lake_count",):
        if col in df.columns:
            df[col] = df[col].astype(int)
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


class _ComparisonExceedanceQuery:
    name = "comparison.exceedance"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, sample_ids: set[int] | None = None,
        comparison_dir: Path | None = None, data_dir: Path | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "comparison" / f"exceedance_grid_agg_r{resolution}.parquet"
        
        comp_dir = comparison_dir or data_dir
        if comp_dir is None:
            raise ValueError("comparison_dir or data_dir is required")
        
        sample_filter = ""
        if sample_ids is not None:
            id_list = ",".join(map(str, sample_ids))
            sample_filter = f"AND q.hylak_id IN ({id_list})"
        
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            WITH quantile_agg AS (
                SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                       FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                       COUNT(DISTINCT q.hylak_id)                    AS lake_count,
                       SUM(CASE WHEN q.extreme_label = 'extreme_high' THEN 1 ELSE 0 END) AS q_high_count,
                       SUM(CASE WHEN q.extreme_label = 'extreme_low'  THEN 1 ELSE 0 END) AS q_low_count,
                       COUNT(*)                                       AS q_total_months
                FROM read_parquet('{comp_dir}/comparison_labels.parquet') q
                JOIN lake_info l ON l.hylak_id = q.hylak_id
                WHERE 1=1 {sample_filter}
                GROUP BY 1, 2
            ),
            pwm_agg AS (
                SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                       FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                       COUNT(DISTINCT la.hylak_id)                AS pwm_lake_count,
                       SUM(CASE WHEN la.water_area > t.threshold_high THEN 1 ELSE 0 END) AS pwm_high_count,
                       SUM(CASE WHEN la.water_area < t.threshold_low  THEN 1 ELSE 0 END) AS pwm_low_count,
                       COUNT(*)                                     AS pwm_total_months
                FROM lake_area la
                JOIN read_parquet('{comp_dir}/comparison_thresholds.parquet') t
                  ON t.hylak_id = la.hylak_id AND t.month = MONTH(la.year_month)
                JOIN lake_info l ON l.hylak_id = la.hylak_id
                WHERE t.converged = true {sample_filter}
                GROUP BY 1, 2
            )
            SELECT qa.cell_lat,
                   qa.cell_lon,
                   qa.lake_count,
                   qa.q_high_count,
                   qa.q_low_count,
                   qa.q_total_months,
                   pa.pwm_high_count,
                   pa.pwm_low_count,
                   pa.pwm_total_months,
                   qa.q_high_count::float / NULLIF(qa.q_total_months, 0) AS q_high_rate,
                   qa.q_low_count::float  / NULLIF(qa.q_total_months, 0) AS q_low_rate,
                   pa.pwm_high_count::float / NULLIF(pa.pwm_total_months, 0) AS pwm_high_rate,
                   pa.pwm_low_count::float  / NULLIF(pa.pwm_total_months, 0) AS pwm_low_rate,
                   (pa.pwm_high_count::float / NULLIF(pa.pwm_total_months, 0))
                     - (qa.q_high_count::float / NULLIF(qa.q_total_months, 0)) AS diff_high_rate,
                   (pa.pwm_low_count::float / NULLIF(pa.pwm_total_months, 0))
                     - (qa.q_low_count::float / NULLIF(qa.q_total_months, 0)) AS diff_low_rate
            FROM quantile_agg qa
            JOIN pwm_agg pa ON pa.cell_lat = qa.cell_lat AND pa.cell_lon = qa.cell_lon
            ORDER BY 1, 2
            """)
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, sample_ids: set[int] | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "comparison.exceedance is only implemented for Parquet backend"
        )


register_grid_query(_ComparisonExceedanceQuery())