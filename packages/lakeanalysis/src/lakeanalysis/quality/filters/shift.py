"""Structural shift filter.

Implements a lightweight change-point detector inspired by the project
algorithm note:

* optionally deseason by month climatology when seasonality dominates
* search for a dominant single breakpoint with an HAC-adjusted supF score
* use a conditional second-break check to separate degraded from intermittent
  lakes
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.stats import f

from . import AnomalyFlag, LakeContext


@dataclass(frozen=True)
class ShiftConfig:
    """Config for structural-shift filter.

    Attributes:
        p_value_thresh: Significance threshold (default 0.05).
        smooth_window: Rolling-smooth window in months (default 12).
        min_segment_months: Minimum segment length for a candidate breakpoint.
        seasonality_threshold: SDR threshold for deseasoning.
        udmax_critical: Approximate 5% critical value for UDmax.
        wdmax_critical: Approximate 5% critical value for WDmax.
    """

    p_value_thresh: float = 0.05
    smooth_window: int = 12
    min_segment_months: int = 24
    seasonality_threshold: float = 0.3
    udmax_critical: float = 8.88
    wdmax_critical: float = 9.91


@dataclass(frozen=True)
class _BreakpointResult:
    index: int | None
    stat: float
    p_value: float
    left_mean: float
    right_mean: float
    left_max: float
    right_max: float


def _ordered_frame(ctx: LakeContext) -> pd.DataFrame:
    frame = ctx.df_no_frozen.copy()
    if {"year", "month"}.issubset(frame.columns):
        frame = frame.sort_values(["year", "month"], kind="mergesort")
    return frame.reset_index(drop=True)


def _monthly_climatology(values: pd.Series, months: pd.Series) -> np.ndarray:
    mean_by_month = values.groupby(months).mean()
    overall_mean = float(values.mean())
    return months.map(mean_by_month).to_numpy(dtype=float) - overall_mean


def _seasonality_dominance_ratio(values: pd.Series, months: pd.Series) -> float:
    if values.empty:
        return 0.0
    if months.value_counts().min() < 2:
        return 0.0
    seasonal = _monthly_climatology(values, months)
    denom = float(np.var(values.to_numpy(dtype=float), ddof=0))
    if denom <= 0:
        return 0.0
    return float(np.var(seasonal, ddof=0) / denom)


def _deseason_by_month(values: pd.Series, months: pd.Series) -> np.ndarray:
    seasonal = _monthly_climatology(values, months)
    return values.to_numpy(dtype=float) - seasonal


def _hac_variance(values: np.ndarray, lag: int) -> float:
    centered = np.asarray(values, dtype=float) - float(np.mean(values))
    n = centered.size
    if n == 0:
        return 0.0
    gamma0 = float(np.mean(centered * centered))
    if lag <= 0:
        return gamma0

    long_run = gamma0
    max_lag = min(lag, n - 1)
    for k in range(1, max_lag + 1):
        weight = 1.0 - (k / (max_lag + 1.0))
        gamma_k = float(np.mean(centered[k:] * centered[:-k]))
        long_run += 2.0 * weight * gamma_k
    return max(long_run, 1e-12)


def _supf_one(values: np.ndarray, min_segment: int, lag: int) -> _BreakpointResult:
    n = int(values.size)
    if n < 2 * min_segment + 1:
        mean = float(np.mean(values)) if n else 0.0
        vmax = float(np.max(values)) if n else 0.0
        return _BreakpointResult(None, 0.0, 1.0, mean, mean, vmax, vmax)

    overall_mean = float(np.mean(values))
    restricted_sse = float(np.sum((values - overall_mean) ** 2))
    best = _BreakpointResult(None, 0.0, 1.0, overall_mean, overall_mean, float(np.max(values)), float(np.max(values)))

    for split in range(min_segment, n - min_segment + 1):
        left = values[:split]
        right = values[split:]
        left_mean = float(np.mean(left))
        right_mean = float(np.mean(right))
        left_sse = float(np.sum((left - left_mean) ** 2))
        right_sse = float(np.sum((right - right_mean) ** 2))
        candidate_sse = left_sse + right_sse
        improvement = max(restricted_sse - candidate_sse, 0.0)
        residuals = np.concatenate([left - left_mean, right - right_mean])
        hac_var = _hac_variance(residuals, lag=lag)
        scale = max(hac_var * n, 1e-12)
        stat = improvement / scale
        df2 = max(n - 2 * min_segment - 1, 1)
        p_value = float(f.sf(stat, 1, df2))

        if stat > best.stat:
            best = _BreakpointResult(
                index=split,
                stat=stat,
                p_value=p_value,
                left_mean=left_mean,
                right_mean=right_mean,
                left_max=float(np.max(left)),
                right_max=float(np.max(right)),
            )

    return best


def _supf_two_given_one(values: np.ndarray, split: int, min_segment: int, lag: int) -> _BreakpointResult:
    left = _supf_one(values[:split], min_segment=min_segment, lag=lag)
    right = _supf_one(values[split:], min_segment=min_segment, lag=lag)
    if left.stat >= right.stat:
        index = left.index
        if index is not None:
            index = int(index)
        return _BreakpointResult(
            index=index,
            stat=left.stat,
            p_value=left.p_value,
            left_mean=left.left_mean,
            right_mean=left.right_mean,
            left_max=left.left_max,
            right_max=left.right_max,
        )
    index = right.index
    if index is not None:
        index = int(index + split)
    return _BreakpointResult(
        index=index,
        stat=right.stat,
        p_value=right.p_value,
        left_mean=right.left_mean,
        right_mean=right.right_mean,
        left_max=right.left_max,
        right_max=right.right_max,
    )


def _alternating_regime_break(values: np.ndarray, min_segment: int) -> int | None:
    if values.size < 2 * min_segment + 1:
        return None
    median = float(np.median(values))
    labels = values >= median
    changes = int(np.sum(labels[1:] != labels[:-1]))
    if changes < 2:
        return None
    true_count = int(np.sum(labels))
    false_count = int(labels.size - true_count)
    if true_count >= min_segment and false_count >= min_segment:
        first_change = int(np.flatnonzero(labels[1:] != labels[:-1])[0] + 1)
        return first_change
    return None


class ShiftFilter:
    name = "shift"

    def __init__(self, config: ShiftConfig | None = None) -> None:
        self._config = config or ShiftConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        frame = _ordered_frame(ctx)
        values = pd.to_numeric(frame["water_area"], errors="coerce").dropna()
        if values.empty:
            return AnomalyFlag(name=self.name, is_anomaly=False, detail={"label": "stable"})

        aligned = frame.loc[values.index]
        months = pd.to_numeric(aligned["month"], errors="coerce").astype(int)
        raw_values = values.to_numpy(dtype=float)

        sdr = _seasonality_dominance_ratio(values, months)
        working = _deseason_by_month(values, months) if sdr > self._config.seasonality_threshold else raw_values
        lag = max(0, min(self._config.smooth_window, working.size // 4))
        min_segment = max(1, min(self._config.min_segment_months, working.size // 2))

        udmax = _supf_one(working, min_segment=min_segment, lag=lag)
        has_single_break = udmax.index is not None and (
            udmax.stat >= self._config.udmax_critical or udmax.p_value <= self._config.p_value_thresh
        )

        label = "stable"
        is_anomaly = False
        supf2 = _BreakpointResult(None, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0)

        if has_single_break:
            supf2 = _supf_two_given_one(working, udmax.index, min_segment=min_segment, lag=lag)
            has_second_break = supf2.stat >= self._config.wdmax_critical or supf2.p_value <= self._config.p_value_thresh
            if has_second_break:
                label = "intermittent"
            else:
                label = "degraded"
                is_anomaly = True
        else:
            alternating_break = _alternating_regime_break(working, min_segment=min_segment)
            if alternating_break is not None:
                label = "intermittent"
                supf2 = _BreakpointResult(
                    index=alternating_break,
                    stat=0.0,
                    p_value=1.0,
                    left_mean=float(np.mean(working[:alternating_break])),
                    right_mean=float(np.mean(working[alternating_break:])),
                    left_max=float(np.max(working[:alternating_break])),
                    right_max=float(np.max(working[alternating_break:])),
                )

        detail = {
            "label": label,
            "seasonality_dominance_ratio": sdr,
            "used_deseasoned": sdr > self._config.seasonality_threshold,
            "udmax": udmax.stat,
            "udmax_p_value": udmax.p_value,
            "udmax_break_index": udmax.index,
            "wdmax": supf2.stat,
            "wdmax_p_value": supf2.p_value,
            "wdmax_break_index": supf2.index,
        }
        if udmax.index is not None:
            detail.update(
                {
                    "pre_break_mean": udmax.left_mean,
                    "post_break_mean": udmax.right_mean,
                    "pre_break_max": udmax.left_max,
                    "post_break_max": udmax.right_max,
                }
            )

        return AnomalyFlag(name=self.name, is_anomaly=is_anomaly, detail=detail)
