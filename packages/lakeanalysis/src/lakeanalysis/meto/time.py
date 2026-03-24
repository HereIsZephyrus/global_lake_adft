"""Continuous monthly time axis (aligned with ``eot.preprocess.MonthlyTimeSeries``)."""

from __future__ import annotations

import numpy as np
import pandas as pd


def continuous_time_from_year_month(
    year: int | np.ndarray | pd.Series,
    month: int | np.ndarray | pd.Series,
    start_year: int,
) -> np.ndarray | float:
    """Map calendar year/month to EOT-style continuous time in years.

    Formula matches ``MonthlyTimeSeries.from_frame``::

        time = (year - start_year) + (month - 1) / 12

    Args:
        year: Calendar year(s).
        month: Month(s) in 1..12.
        start_year: Origin year (use the same as the lake series for alignment).

    Returns:
        Scalar or ndarray of float times.
    """
    y = np.asarray(year, dtype=float)
    m = np.asarray(month, dtype=float)
    return (y - float(start_year)) + (m - 1.0) / 12.0


def normalize_monthly_index(
    df: pd.DataFrame,
    *,
    year_col: str = "year",
    month_col: str = "month",
    start_year: int | None = None,
    hylak_col: str | None = "hylak_id",
) -> pd.DataFrame:
    """Return a copy with integer ``year``/``month`` and computed ``time`` column.

    If ``start_year`` is None, use ``df[year_col].min()`` (per-frame origin), matching
    EOT behaviour for a single-lake frame. For multi-lake meteo joins, pass an explicit
    ``start_year`` shared with the lake area series.

    Args:
        df: Input frame.
        year_col: Year column name.
        month_col: Month column name (1..12).
        start_year: Time origin; default min year in frame.
        hylak_col: If present, sort by this column then year/month.

    Returns:
        DataFrame with ``year``, ``month``, ``time`` columns (and other columns preserved).
    """
    if year_col not in df.columns or month_col not in df.columns:
        raise ValueError(f"DataFrame must contain {year_col!r} and {month_col!r}")

    out = df.copy()
    out[year_col] = out[year_col].astype(int)
    out[month_col] = out[month_col].astype(int)
    invalid = ~out[month_col].between(1, 12)
    if invalid.any():
        bad = out.loc[invalid, month_col].head(5).tolist()
        raise ValueError(f"month must be in 1..12; offending values (sample): {bad}")

    origin = int(out[year_col].min()) if start_year is None else int(start_year)
    out["time"] = continuous_time_from_year_month(
        out[year_col].to_numpy(),
        out[month_col].to_numpy(),
        origin,
    )

    sort_keys = [year_col, month_col]
    if hylak_col and hylak_col in out.columns:
        sort_keys = [hylak_col, year_col, month_col]
    out = out.sort_values(sort_keys).reset_index(drop=True)
    return out
