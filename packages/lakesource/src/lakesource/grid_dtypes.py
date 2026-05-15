"""Shared dtype normalization helpers for grid aggregation outputs."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def coerce_grid_columns(
    df: pd.DataFrame,
    *,
    int_columns: Iterable[str] = (),
    float_columns: Iterable[str] = (),
) -> pd.DataFrame:
    """Normalize common grid coordinates plus requested numeric output columns."""
    for column in ("cell_lat", "cell_lon"):
        if column in df.columns:
            df[column] = df[column].astype(float)

    for column in int_columns:
        if column in df.columns:
            df[column] = df[column].astype(int)

    for column in float_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype(float)

    return df
