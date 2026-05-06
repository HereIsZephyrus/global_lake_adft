"""Penalized volatility anomaly filter (computed after removing frozen months)."""

from __future__ import annotations

from dataclasses import dataclass

from ..metrics import compute_penalized_volatility
from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class PenalizedVolatilityConfig:
    """Config for penalized-volatility flat-series filter.

    Attributes:
        pv_threshold: Flag when penalized_volatility <= this threshold.
        dominant_ratio_max: Flag when dominant_ratio (defrozen) >= this value.
    """

    pv_threshold: float = 0.002
    dominant_ratio_max: float = 1.0


class PenalizedVolatilityFilter:
    name = "pv"

    def __init__(self, config: PenalizedVolatilityConfig | None = None) -> None:
        self._config = config or PenalizedVolatilityConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        metrics = compute_penalized_volatility(ctx.df_no_frozen["water_area"])
        pv = metrics["penalized_volatility"]
        dr = metrics["dominant_ratio"]
        is_anomaly = (pv <= self._config.pv_threshold) or (dr >= self._config.dominant_ratio_max)
        return AnomalyFlag(
            name=self.name,
            is_anomaly=is_anomaly,
            detail={
                "penalized_volatility": pv,
                "pv_dominant_ratio": dr,
                "n_zero_delta": metrics["n_zero_delta"],
                "std_pct_change": metrics["std_pct_change"],
            },
        )
