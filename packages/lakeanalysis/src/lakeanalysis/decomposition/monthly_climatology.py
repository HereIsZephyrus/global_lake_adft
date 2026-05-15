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
from .series import normalize_monthly_series


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

        df = normalize_monthly_series(series_df)

        climatology = df.groupby("month", as_index=False)["water_area"].mean()
        climatology = climatology.rename(columns={"water_area": "monthly_climatology"})

        df = df.merge(climatology, on="month", how="left", validate="many_to_one")
        df["index_value"] = df["water_area"] - df["monthly_climatology"]
        df = df.drop(columns=["monthly_climatology"])

        return DecompositionResult(
            index_df=df.reset_index(drop=True),
            metadata={"method": "legacy"},
        )
