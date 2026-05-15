"""Monthly climatology decomposition (legacy, deprecated).

This method computes ``index_value = water_area - monthly_mean_area``,
i.e. the same anomaly currently used by the quantile workflow.

.. deprecated::
    Use :class:`STLPercentileMethod` instead.  This method does **not**
    remove long-term trends or handle multiplicative variance structure.
    It is retained for backward-compatibility via ``method="legacy"``.
"""

from __future__ import annotations

import warnings

import pandas as pd

from .base import DecompositionResult

REQUIRED_COLUMNS = ("year", "month", "water_area")


class MonthlyClimatologyMethod:
    """Legacy anomaly = water_area - monthly climatology."""

    @property
    def method_name(self) -> str:
        return "legacy"

    def decompose(self, series_df: pd.DataFrame) -> DecompositionResult:
        warnings.warn(
            "MonthlyClimatologyMethod is deprecated. Switch to STLPercentileMethod.",
            DeprecationWarning,
            stacklevel=2,
        )

        missing = [c for c in REQUIRED_COLUMNS if c not in series_df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

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

        climatology = df.groupby("month", as_index=False)["water_area"].mean()
        climatology = climatology.rename(columns={"water_area": "monthly_climatology"})

        df = df.merge(climatology, on="month", how="left", validate="many_to_one")
        df["index_value"] = df["water_area"] - df["monthly_climatology"]
        df = df.drop(columns=["monthly_climatology"])

        return DecompositionResult(
            index_df=df.reset_index(drop=True),
            metadata={"method": "legacy"},
        )
