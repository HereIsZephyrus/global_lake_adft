"""Penalized volatility anomaly filter using H×CV (entropy-weighted coefficient of variation).

Computed after removing frozen months. Replaces the old
std_pct_change / sqrt(n_zero_delta) metric.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..metrics import compute_penalized_volatility
from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class PenalizedVolatilityConfig:
    """Config for H×CV flat-series filter.

    Attributes:
        pv_threshold: Flag when H×CV <= this threshold (default 0.001).
    """

    pv_threshold: float = 0.001


class PenalizedVolatilityFilter:
    name = "pv"

    def __init__(self, config: PenalizedVolatilityConfig | None = None) -> None:
        self._config = config or PenalizedVolatilityConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        metrics = compute_penalized_volatility(ctx.df_no_frozen["water_area"])
        pv = metrics["penalized_volatility"]
        is_anomaly = pv is not None and pv <= self._config.pv_threshold
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={
                "penalized_volatility": pv,
                "h_cv": metrics["h_cv"],
                "H": metrics["H"],
                "cv": metrics["cv"],
                "pv_dominant_ratio": metrics["dominant_ratio"],
                "n_distinct": metrics["n_distinct"],
            },
        )
