"""Median-zero anomaly filter."""

from __future__ import annotations

from . import AnomalyFilter, AnomalyFlag, LakeContext


class MedianZeroFilter:
    name = "median_zero"

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        is_anomaly = ctx.rs_area_median == 0.0
        return AnomalyFlag(name=self.name, is_anomaly=is_anomaly, detail={})
