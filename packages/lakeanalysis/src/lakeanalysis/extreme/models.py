"""Shared domain models for extreme-event workflows."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class ExtremeResult:
    """Shared core output of any extreme-event workflow for one lake."""

    hylak_id: int | None
    labels_df: pd.DataFrame
    extremes_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    transitions_df: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass(frozen=True)
class QuantileDiagnostics:
    """Quantile-specific threshold diagnostics."""

    q_low: float
    q_high: float


@dataclass(frozen=True)
class PWMDiagnostics:
    """PWM-specific threshold diagnostics."""

    month_results: list

    @property
    def thresholds_df(self) -> pd.DataFrame:
        """Return a tidy DataFrame of monthly thresholds from diagnostics."""
        return pd.DataFrame(
            [
                {
                    "hylak_id": mr.hylak_id,
                    "threshold_quantile": mr.threshold_quantile,
                    "month": mr.month,
                    "mean_area": mr.mean_area,
                    "epsilon": mr.epsilon,
                    "threshold_high": mr.threshold_high,
                    "threshold_low": mr.threshold_low,
                    "converged": mr.converged,
                    "objective_value": mr.objective_value,
                }
                for mr in self.month_results
            ]
        )
