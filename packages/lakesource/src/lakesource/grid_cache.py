"""Shared parquet-backed cache helpers for grid aggregation queries."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

import pandas as pd


def cached_or_compute(
    cache_path: Path,
    *,
    refresh: bool,
    compute_fn: Callable[[], pd.DataFrame],
    log: logging.Logger,
) -> pd.DataFrame:
    """Return cached grid data or compute and persist it."""
    if not refresh and cache_path.exists():
        log.info("Loading from cache: %s", cache_path)
        return pd.read_parquet(cache_path)

    df = compute_fn()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(cache_path, index=False)
    log.info("Cached %d rows to %s", len(df), cache_path)
    return df
