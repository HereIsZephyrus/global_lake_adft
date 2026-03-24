"""Aggregate daily meteorological tables to monthly summaries for lake alignment."""

from __future__ import annotations

import pandas as pd


def aggregate_daily_meteo_to_monthly(
    df: pd.DataFrame,
    *,
    date_col: str = "date",
    hylak_col: str = "hylak_id",
    sum_columns: list[str] | None = None,
    mean_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Group by lake and calendar month; sum or mean selected value columns.

    Typical use: daily precipitation totals summed within month; daily mean temperature
    averaged again over days (unweighted; for strict physical monthly means consider
    weighting by interval length).

    Args:
        df: Must contain ``date_col`` (parseable as datetimes) and ``hylak_col``.
        date_col: Column with daily dates.
        hylak_col: Lake identifier column.
        sum_columns: Columns to aggregate with ``sum`` (e.g. daily precipitation depth).
        mean_columns: Columns to aggregate with ``mean`` (e.g. daily mean 2 m temperature).

    Returns:
        DataFrame with ``hylak_col``, ``year``, ``month``, and aggregated columns.
    """
    if sum_columns is None:
        sum_columns = []
    if mean_columns is None:
        mean_columns = []

    required = {date_col, hylak_col}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {sorted(missing)}")

    for col in sum_columns + mean_columns:
        if col not in df.columns:
            raise ValueError(f"Column {col!r} not in DataFrame")

    work = df.copy()
    work["_dt"] = pd.to_datetime(work[date_col], utc=True, errors="coerce")
    if work["_dt"].isna().any():
        raise ValueError(f"Invalid dates in column {date_col!r}")
    work["year"] = work["_dt"].dt.year
    work["month"] = work["_dt"].dt.month

    agg: dict[str, str] = {}
    for c in sum_columns:
        agg[c] = "sum"
    for c in mean_columns:
        agg[c] = "mean"

    if not agg:
        raise ValueError("At least one of sum_columns or mean_columns must be non-empty")

    return work.groupby([hylak_col, "year", "month"], as_index=False).agg(agg)
