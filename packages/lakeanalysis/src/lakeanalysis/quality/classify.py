"""Anomaly classification aggregator."""

from __future__ import annotations

from .filters import (
    AnomalyFilter,
    AnomalyFlag,
    LakeContext,
    FLAG_MEDIAN_ZERO,
    FLAG_FLAT,
    FLAG_AREA_RATIO,
    FLAG_OUTSIDE_RANGE,
    FLAG_PV,
    FLAG_NAMES,
    decode_anomaly_flags,
    encode_anomaly_flags,
)
from .filters.median_zero import MedianZeroFilter
from .filters.flatness import FlatnessFilter, FlatnessFilterConfig
from .filters.area_ratio import AreaRatioFilter, AreaRatioConfig
from .filters.outside_range import OutsideRangeFilter, OutsideRangeConfig
from .filters.penalized_volatility import PenalizedVolatilityFilter, PenalizedVolatilityConfig


_FLAG_BITS: dict[str, int] = {
    "median_zero": FLAG_MEDIAN_ZERO,
    "flat": FLAG_FLAT,
    "area_ratio": FLAG_AREA_RATIO,
    "outside_range": FLAG_OUTSIDE_RANGE,
    "pv": FLAG_PV,
}


def classify_area_anomaly(
    ctx: LakeContext,
    filters: list[AnomalyFilter],
) -> dict[str, bool | float | int]:
    """Run all filters and merge results into a flat dict.

    Returns:
        Dict with:
          - is_anomalous: True if any filter flags anomaly
          - anomaly_flags: integer bitmask of triggered filters
          - is_{filter.name}: bool per filter
          - {detail keys}: merged from all filter details
    """
    flags: list[AnomalyFlag] = [f.classify(ctx) for f in filters]
    anomaly_flags = 0
    result: dict[str, bool | float | int] = {
        "is_anomalous": any(f.is_anomaly for f in flags),
    }
    for f in flags:
        result[f"is_{f.name}"] = f.is_anomaly
        if f.is_anomaly and f.name in _FLAG_BITS:
            anomaly_flags |= _FLAG_BITS[f.name]
        result.update(f.detail)
    result["anomaly_flags"] = anomaly_flags
    return result


def default_filters(
    flat_config: FlatnessFilterConfig | None = None,
    ratio_config: AreaRatioConfig | None = None,
    pv_config: PenalizedVolatilityConfig | None = None,
    outside_range_config: OutsideRangeConfig | None = None,
) -> list[AnomalyFilter]:
    """Construct the default filter chain."""
    return [
        MedianZeroFilter(),
        FlatnessFilter(flat_config),
        AreaRatioFilter(ratio_config),
        OutsideRangeFilter(outside_range_config),
        PenalizedVolatilityFilter(pv_config),
    ]
