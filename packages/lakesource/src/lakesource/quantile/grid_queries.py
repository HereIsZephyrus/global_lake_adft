"""Quantile grid aggregation queries registered via grid_query protocol."""

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
    for col in ("lake_count", "event_count"):
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


class _QuantileExtremesQuery:
    name = "quantile.extremes"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "quantile" / f"extremes_grid_agg_r{resolution}.parquet"
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT e.hylak_id)                    AS lake_count,
                   COUNT(*)                                       AS event_count
            FROM   quantile_extremes e
            JOIN   lake_info l ON l.hylak_id = e.hylak_id
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_extremes_grid_agg
        return fetch_extremes_grid_agg(config, resolution, refresh=refresh)


class _QuantileExtremesByTypeQuery:
    name = "quantile.extremes_by_type"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "quantile" / f"extremes_by_type_grid_agg_r{resolution}.parquet"
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT e.event_type,
                   FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT e.hylak_id)                    AS lake_count,
                   COUNT(*)                                       AS event_count
            FROM   quantile_extremes e
            JOIN   lake_info l ON l.hylak_id = e.hylak_id
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """)
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_extremes_by_type_grid_agg
        return fetch_extremes_by_type_grid_agg(config, resolution, refresh=refresh)


class _QuantileTransitionsQuery:
    name = "quantile.transitions"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "quantile" / f"transitions_grid_agg_r{resolution}.parquet"
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT t.hylak_id)                    AS lake_count,
                   COUNT(*)                                       AS event_count
            FROM   quantile_abrupt_transitions t
            JOIN   lake_info l ON l.hylak_id = t.hylak_id
            GROUP BY 1, 2
            ORDER BY 1, 2
            """)
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_transitions_grid_agg
        return fetch_transitions_grid_agg(config, resolution, refresh=refresh)


class _QuantileTransitionsByTypeQuery:
    name = "quantile.transitions_by_type"

    def fetch_parquet(
        self, client: Any, cache_dir: Path, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        cache = cache_dir / "quantile" / f"transitions_by_type_grid_agg_r{resolution}.parquet"
        return _cached_or_compute(cache, refresh, lambda: _fix_grid_dtypes(
            client.query_df(f"""
            SELECT t.transition_type,
                   FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
                   FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
                   COUNT(DISTINCT t.hylak_id)                    AS lake_count,
                   COUNT(*)                                       AS event_count
            FROM   quantile_abrupt_transitions t
            JOIN   lake_info l ON l.hylak_id = t.hylak_id
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
            """)
        ))

    def fetch_postgres(
        self, config: Any, resolution: float,
        *, refresh: bool = False, **kwargs: Any,
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_transitions_by_type_grid_agg
        return fetch_transitions_by_type_grid_agg(config, resolution, refresh=refresh)


register_grid_query(_QuantileExtremesQuery())
register_grid_query(_QuantileExtremesByTypeQuery())
register_grid_query(_QuantileTransitionsQuery())
register_grid_query(_QuantileTransitionsByTypeQuery())
