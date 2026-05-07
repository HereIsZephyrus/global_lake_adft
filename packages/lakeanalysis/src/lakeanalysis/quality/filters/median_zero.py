"""Zero-quantile anomaly filter.

Flags a lake as anomalous when a configurable quantile of its defrozen
water_area is zero.  Using a quantile higher than 0.5 (the median) avoids
misclassifying seasonally dry lakes where the median is zero but the lake
still has meaningful non-zero observations.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class ZeroQuantileConfig:
    """Config for zero-quantile filter.

    Attributes:
        quantile: Quantile position (0-1) at which to check for zero area.
            Default 0.75 means flag only when the 75th percentile is zero,
            i.e. at least 75% of observations are zero.
    """

    quantile: float = 0.80


class ZeroQuantileFilter:
    name = "zero_quantile"

    def __init__(self, config: ZeroQuantileConfig | None = None) -> None:
        self._config = config or ZeroQuantileConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        is_anomaly = ctx.rs_area_quantile == 0.0
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={"zero_quantile": self._config.quantile},
        )


# Backward-compatible aliases
MedianZeroFilter = ZeroQuantileFilter
