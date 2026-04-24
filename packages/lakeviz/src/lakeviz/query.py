"""Data loading for lakeviz via lakesource."""

from __future__ import annotations

import pandas as pd

from lakesource.monthly_transition.reader import (
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
