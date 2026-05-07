"""Tests for lakeanalysis.quality.filters.shift."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.quality.filters.shift import ShiftConfig, ShiftFilter
from lakeanalysis.quality.filters import LakeContext


def _make_ctx(values: list[float]) -> LakeContext:
    n = len(values)
    df = pd.DataFrame(
        {
            "year": [2000 + (i // 12) for i in range(n)],
            "month": [(i % 12) + 1 for i in range(n)],
            "water_area": values,
        }
    )
    return LakeContext(
        df=df,
        df_no_frozen=df.copy(),
        rs_area_median=float(pd.Series(values).median()),
        rs_area_mean=float(pd.Series(values).mean()),
        rs_area_quantile=float(pd.Series(values).quantile(0.8)),
        atlas_area=0.0,
    )


def test_shift_filter_stable_series() -> None:
    filt = ShiftFilter(ShiftConfig(min_segment_months=3, smooth_window=3))
    ctx = _make_ctx([10.0] * 12)

    result = filt.classify(ctx)

    assert result.is_anomaly is False
    assert result.detail["label"] == "stable"
    assert result.detail["udmax_break_index"] is None


def test_shift_filter_degraded_series() -> None:
    filt = ShiftFilter(ShiftConfig(min_segment_months=3, smooth_window=3))
    ctx = _make_ctx([100.0] * 6 + [10.0] * 6)

    result = filt.classify(ctx)

    assert result.is_anomaly is True
    assert result.detail["label"] == "degraded"
    assert result.detail["udmax_break_index"] is not None
    assert result.detail["pre_break_mean"] > result.detail["post_break_mean"]


def test_shift_filter_intermittent_series() -> None:
    filt = ShiftFilter(ShiftConfig(min_segment_months=3, smooth_window=3))
    ctx = _make_ctx([0.0] * 3 + [50.0] * 3 + [0.0] * 3 + [50.0] * 3)

    result = filt.classify(ctx)

    assert result.is_anomaly is False
    assert result.detail["label"] == "intermittent"
    assert result.detail["wdmax_break_index"] is not None
