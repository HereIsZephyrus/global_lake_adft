"""Data loading for lakeviz via lakesource."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakesource.eot.reader import fetch_eot_results_with_coords
from lakesource.quantile.reader import (
    fetch_extremes_with_coords,
    fetch_transitions_with_coords,
)

from .config import GlobalGridConfig


def load_extremes_grid_data(config: GlobalGridConfig) -> pd.DataFrame:
    """Load extremes event data with lake coordinates.

    Returns:
        DataFrame with columns: hylak_id, lat, lon, event_type, year, month.
    """
    return fetch_extremes_with_coords(config.source)


def load_transitions_grid_data(config: GlobalGridConfig) -> pd.DataFrame:
    """Load abrupt transition event data with lake coordinates.

    Returns:
        DataFrame with columns: hylak_id, lat, lon, transition_type, from_year, from_month.
    """
    return fetch_transitions_with_coords(config.source)


def load_eot_results_grid_data(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
) -> pd.DataFrame:
    """Load EOT results with lake coordinates (with parquet cache).

    Args:
        config: Grid visualization config.
        tail: "high" or "low".
        threshold_quantile: Quantile level (e.g. 0.95).
        refresh: Re-fetch from database.
        data_dir: Override cache directory.

    Returns:
        DataFrame with EOT result columns plus lat, lon.
    """
    return fetch_eot_results_with_coords(
        config.source, tail, threshold_quantile,
        refresh=refresh, data_dir=data_dir,
    )
