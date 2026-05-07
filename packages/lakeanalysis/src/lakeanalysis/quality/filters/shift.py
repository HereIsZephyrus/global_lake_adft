"""Structural shift filter (placeholder).

The current implementation is a stub that never flags anomalies.
A change-point detection algorithm will be implemented here in a future iteration.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import AnomalyFilter, AnomalyFlag, LakeContext


@dataclass(frozen=True)
class ShiftConfig:
    """Config for structural-shift filter.

    Attributes:
        p_value_thresh: Significance threshold (default 0.05).
        smooth_window: Rolling-smooth window in months (default 12).
    """

    p_value_thresh: float = 0.05
    smooth_window: int = 12


class ShiftFilter:
    name = "shift"

    def __init__(self, config: ShiftConfig | None = None) -> None:
        self._config = config or ShiftConfig()

    def classify(self, ctx: LakeContext) -> AnomalyFlag:
        return AnomalyFlag(
            name=self.name,
            is_anomaly=False,
            detail={},
        )
