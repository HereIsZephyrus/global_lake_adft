"""Align meteorological (or other) monthly features with lake monthly series."""

from __future__ import annotations

import pandas as pd

from .time import continuous_time_from_year_month, normalize_monthly_index


def align_meteo_to_lake_monthly(
    lake_df: pd.DataFrame,
    meteo_df: pd.DataFrame,
    *,
    on: tuple[str, str, str] = ("hylak_id", "year", "month"),
    how: str = "left",
    start_year: int | None = None,
    drop_meteo_duplicate_keys: bool = True,
) -> pd.DataFrame:
    """Join meteo features onto lake rows on ``(hylak_id, year, month)``.

    Ensures ``time`` on the result matches EOT when ``start_year`` equals the
    minimum year in ``lake_df`` (default) or an explicit shared origin.

    Args:
        lake_df: Lake-side frame with at least ``year``, ``month`` and join keys.
        meteo_df: Feature frame with the same key columns.
        on: Triple (hylak_col, year_col, month_col).
        how: Pandas merge mode (default left-keep all lake months).
        start_year: Continuous-time origin; default ``lake_df[year].min()``.
        drop_meteo_duplicate_keys: If True, drop duplicate keys on meteo side (keep first).

    Returns:
        Merged DataFrame with ``time`` computed from ``start_year``.

    Raises:
        ValueError: On duplicate (hylak_id, year, month) in the merged result.
    """
    hylak_col, year_col, month_col = on
    for name, frame in (("lake_df", lake_df), ("meteo_df", meteo_df)):
        for col in on:
            if col not in frame.columns:
                raise ValueError(f"{name} missing column {col!r}")

    if start_year is None:
        start_year = int(lake_df[year_col].min())

    lake_n = normalize_monthly_index(
        lake_df,
        year_col=year_col,
        month_col=month_col,
        start_year=start_year,
        hylak_col=hylak_col,
    )
    meteo_n = normalize_monthly_index(
        meteo_df,
        year_col=year_col,
        month_col=month_col,
        start_year=start_year,
        hylak_col=hylak_col,
    )

    if drop_meteo_duplicate_keys:
        key_df = meteo_n[[hylak_col, year_col, month_col]]
        dup = key_df.duplicated()
        if dup.any():
            meteo_n = meteo_n.loc[~dup].reset_index(drop=True)

    meteo_feats = meteo_n.drop(columns=["time"], errors="ignore")

    merged = lake_n.merge(
        meteo_feats,
        on=[hylak_col, year_col, month_col],
        how=how,
        suffixes=("_lake", "_meteo"),
    )

    if "time" not in merged.columns:
        merged["time"] = continuous_time_from_year_month(
            merged[year_col].to_numpy(),
            merged[month_col].to_numpy(),
            start_year,
        )

    dup_keys = merged.duplicated(subset=[hylak_col, year_col, month_col])
    if dup_keys.any():
        raise ValueError(
            "Duplicate (hylak_id, year, month) rows after merge; "
            "check meteo export or set drop_meteo_duplicate_keys=True"
        )

    return merged.reset_index(drop=True)
