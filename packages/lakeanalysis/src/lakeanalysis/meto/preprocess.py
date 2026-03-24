"""Validate and clean tabular exports from Earth Engine (or similar)."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd


def validate_meteo_export_columns(
    df: pd.DataFrame,
    required: Iterable[str],
    *,
    hylak_col: str = "hylak_id",
) -> None:
    """Raise ``ValueError`` if any required column is missing."""
    req = set(required)
    if hylak_col in req and hylak_col not in df.columns:
        raise ValueError(f"Missing required column {hylak_col!r}")
    missing = req.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")


def preprocess_meteo_export(
    df: pd.DataFrame,
    *,
    required_columns: Iterable[str] | None = None,
    hylak_col: str = "hylak_id",
    year_col: str = "year",
    month_col: str = "month",
    drop_all_na_feature_cols: bool = True,
) -> pd.DataFrame:
    """Sort, drop rows with missing keys, optionally validate schema.

    Args:
        df: Raw export table.
        required_columns: If set, all of these names must appear in ``df``.
        hylak_col: Lake id column.
        year_col: Year column.
        month_col: Month column.
        drop_all_na_feature_cols: If True, drop columns that are all-NaN (often unused bands).

    Returns:
        Cleaned copy.
    """
    if required_columns is not None:
        validate_meteo_export_columns(df, required_columns, hylak_col=hylak_col)

    out = df.copy()
    key_cols = [c for c in (hylak_col, year_col, month_col) if c in out.columns]
    if len(key_cols) < 3:
        raise ValueError(
            f"DataFrame must include {hylak_col!r}, {year_col!r}, {month_col!r}"
        )

    out = out.dropna(subset=key_cols)
    if out.empty:
        return out

    for c in (year_col, month_col, hylak_col):
        if c in out.columns:
            out[c] = out[c].astype(int)

    sort_keys = [hylak_col, year_col, month_col]
    out = out.sort_values(sort_keys).reset_index(drop=True)

    if drop_all_na_feature_cols:
        extra = [c for c in out.columns if c not in key_cols]
        all_na = [c for c in extra if out[c].isna().all()]
        if all_na:
            out = out.drop(columns=all_na)

    return out
