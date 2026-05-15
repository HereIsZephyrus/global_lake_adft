"""Shared monthly-series normalization helpers for decomposition workflows."""

from __future__ import annotations

import pandas as pd

REQUIRED_COLUMNS = ("year", "month", "water_area")


def normalize_monthly_series(series_df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize one-lake monthly series for decomposition."""
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in series_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = series_df.loc[:, list(REQUIRED_COLUMNS)].copy()
    df["year"] = pd.to_numeric(df["year"], errors="raise").astype(int)
    df["month"] = pd.to_numeric(df["month"], errors="raise").astype(int)
    df["water_area"] = pd.to_numeric(df["water_area"], errors="raise").astype(float)

    if ((df["month"] < 1) | (df["month"] > 12)).any():
        raise ValueError("month must be in 1..12")

    if df.duplicated(["year", "month"]).any():
        df = df.drop_duplicates(subset=["year", "month"], keep="first")

    df = df.sort_values(["year", "month"]).reset_index(drop=True)
    df["year_month_key"] = df["year"] * 100 + df["month"]
    df["month_ordinal"] = df["year"] * 12 + (df["month"] - 1)
    return df
