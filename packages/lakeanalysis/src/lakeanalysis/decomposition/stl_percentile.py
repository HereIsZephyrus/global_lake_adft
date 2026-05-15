"""STL-based monthly percentile decomposition (recommended).

Implements the method described in
``docs/research/lake_area_heteroscedasticity_analysis.md``:

1. Log-transform *log_area = ln(water_area)*.
2. STL decomposition (robust LOESS) → residual *R_t*.
3. Monthly empirical percentile rank → ``index_value`` ∈ [0, 100].
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from statsmodels.tsa.seasonal import STL

from .base import DecompositionResult
from .series import normalize_monthly_series

log = logging.getLogger(__name__)

MIN_YEARS = 3


def _compute_monthly_percentiles(
    stl_residual: np.ndarray,
    months: np.ndarray,
) -> np.ndarray:
    """Compute monthly empirical percentile rank for each observation.

    For each calendar month *k*, the percentile of each observation is
    computed against all historical observations of the same month:

        p_t = count(R_{k,i} ≤ R_t) / (Y + 1) × 100
    """
    percentile = np.full(len(stl_residual), np.nan, dtype=float)
    for month in range(1, 13):
        month_mask = months == month
        month_vals = stl_residual[month_mask]
        if len(month_vals) < 2:
            continue
        sorted_vals = np.sort(month_vals)
        n = len(sorted_vals)
        ranks = np.searchsorted(sorted_vals, month_vals, side="right")
        percentile[month_mask] = ranks / (n + 1) * 100.0
    return percentile


class STLPercentileMethod:
    """STL decomposition → monthly percentile rank anomaly index."""

    def __init__(
        self,
        *,
        period: int = 12,
        seasonal: int = 11,
        trend: int | None = None,
        robust: bool = True,
    ) -> None:
        self._period = period
        self._seasonal = seasonal
        self._trend = trend
        self._robust = robust

    @property
    def method_name(self) -> str:
        return "stl"

    def decompose(self, series_df: pd.DataFrame) -> DecompositionResult:
        df = normalize_monthly_series(series_df)

        if (df["water_area"] <= 0).any():
            raise ValueError("STL decomposition requires all water_area > 0")

        years = df["year"].nunique()
        if years < MIN_YEARS:
            raise ValueError(
                f"Need at least {MIN_YEARS} years of data for STL; got {years}"
            )

        n = len(df)
        log_area = np.log(df["water_area"].to_numpy(dtype=float))

        # --- Step 1: heteroscedasticity diagnosis (VR ratio) -------
        monthly_std = np.array([
            np.std(log_area[df["month"].to_numpy(dtype=int) == m])
            for m in range(1, 13)
        ])
        monthly_std = monthly_std[np.isfinite(monthly_std) & (monthly_std > 0)]
        vr = float(monthly_std.max() / monthly_std.min()) if len(monthly_std) > 1 else 1.0

        # --- Step 2: STL decomposition ---------------------------------
        trend_win = self._trend
        if trend_win is None:
            trend_win = max(
                13,
                int(np.ceil(1.5 * self._period / (1.0 - 1.5 / self._seasonal))),
            )
            if trend_win % 2 == 0:
                trend_win += 1

        if trend_win >= n:
            raise ValueError(
                f"STL trend window ({trend_win}) exceeds series length ({n})"
            )

        stl = STL(
            log_area,
            period=self._period,
            seasonal=self._seasonal,
            trend=trend_win,
            robust=self._robust,
        )
        result = stl.fit()
        residual = result.resid.astype(float)

        # --- Step 3: monthly percentile rank ---------------------------
        months_arr = df["month"].to_numpy(dtype=int)
        percentile = _compute_monthly_percentiles(residual, months_arr)

        df["log_area"] = log_area
        df["stl_trend"] = result.trend.astype(float)
        df["stl_seasonal"] = result.seasonal.astype(float)
        df["stl_residual"] = residual
        df["index_value"] = percentile

        return DecompositionResult(
            index_df=df,
            metadata={
                "method": "stl",
                "vr": vr,
                "seasonal": self._seasonal,
                "trend_window": trend_win,
                "robust": self._robust,
            },
        )
