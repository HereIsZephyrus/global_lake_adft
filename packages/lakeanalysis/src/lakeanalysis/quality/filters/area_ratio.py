"""Area-ratio anomaly filter."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from ..metrics import compute_area_ratio
from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class AreaRatioConfig:
    """Config for area-ratio filter.

    Attributes:
        min_ratio: Minimum acceptable ratio (rs_area_median / atlas_area).
        max_ratio: Maximum acceptable ratio (rs_area_median / atlas_area).
    """

    min_ratio: float = 0.1
    max_ratio: float = 10.0


class AreaRatioFilter:
    name = "area_ratio"

    def __init__(self, config: AreaRatioConfig | None = None) -> None:
        self._config = config or AreaRatioConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        ratio = float(
            compute_area_ratio(
                np.asarray([ctx.rs_area_median]),
                np.asarray([ctx.atlas_area]),
            )[0]
        )
        is_anomaly = (
            np.isnan(ratio)
            or ratio < self._config.min_ratio
            or ratio > self._config.max_ratio
        )
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={"area_ratio": ratio},
        )
