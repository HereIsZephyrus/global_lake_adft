"""Flatness anomaly filter."""

from __future__ import annotations

from dataclasses import dataclass

from ..metrics import compute_flatness_metrics
from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class FlatnessFilterConfig:
    """Config for flat-series filters.

    Attributes:
        dominant_ratio_threshold: Flag when most common value frequency / N is
            greater than or equal to this threshold.
        round_digits: Optional rounding digits for value bucketing before
            computing value frequencies.
    """

    dominant_ratio_threshold: float = 0.8
    round_digits: int | None = None


class FlatnessFilter:
    name = "flat"

    def __init__(self, config: FlatnessFilterConfig | None = None) -> None:
        self._config = config or FlatnessFilterConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        metrics = compute_flatness_metrics(
            ctx.df_no_frozen,
            value_column="water_area",
            round_digits=self._config.round_digits,
        )
        dominant_ratio = metrics["dominant_ratio"]
        is_anomaly = dominant_ratio >= self._config.dominant_ratio_threshold
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={"dominant_ratio": dominant_ratio},
        )
