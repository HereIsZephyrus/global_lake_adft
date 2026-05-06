"""Outside-range anomaly filter."""

from __future__ import annotations

from ..metrics import compute_area_range
from . import AnomalyFilter, AnomalyFlag, LakeContext


class OutsideRangeFilter:
    name = "outside_range"

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        range_metrics = compute_area_range(ctx.df_no_frozen)
        min_area = range_metrics["min_area"]
        max_area = range_metrics["max_area"]
        atlas = ctx.atlas_area

        if atlas <= 0:
            is_outside = False
            is_below = False
            is_above = False
        else:
            is_below = atlas < min_area
            is_above = atlas > max_area
            is_outside = is_below or is_above

        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_outside,
            detail={
                "is_below_min": bool(is_below),
                "is_above_max": bool(is_above),
                "min_area": min_area,
                "max_area": max_area,
            },
        )
