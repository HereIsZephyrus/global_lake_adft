"""Outside-range anomaly filter."""

from __future__ import annotations

from dataclasses import dataclass

from ..metrics import compute_area_range
from . import AnomalyFlag, LakeContext


@dataclass(frozen=True)
class OutsideRangeConfig:
    """Config for outside-range filter.

    Attributes:
        tolerance: Fractional tolerance beyond observed range.
            atlas_area must exceed min*(1-tolerance) or max*(1+tolerance)
            to be flagged. 0.0 = strict, 0.5 = allow 50% overshoot.
    """

    tolerance: float = 0.5


class OutsideRangeFilter:
    name = "outside_range"

    def __init__(self, config: OutsideRangeConfig | None = None) -> None:
        self._config = config or OutsideRangeConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        range_metrics = compute_area_range(ctx.df_no_frozen)
        min_area = range_metrics["min_area"] / 1_000_000
        max_area = range_metrics["max_area"] / 1_000_000
        atlas = ctx.atlas_area
        tol = self._config.tolerance

        if atlas <= 0:
            is_outside = False
            is_below = False
            is_above = False
        else:
            is_below = atlas < min_area * (1 - tol)
            is_above = atlas > max_area * (1 + tol)
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
