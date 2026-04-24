"""Shared data types and constants for the PWM extreme quantile workflow.

These schemas are owned by the data layer (lakesource) so that both
lakesource and lakeanalysis can reference them without creating a
circular dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd


CURRENT_PWM_EXTREME_WORKFLOW_VERSION = "pwm-extreme-v1"


@dataclass(frozen=True)
class PWMExtremeMonthResult:
    """PWM extreme quantile result for one lake-month."""

    hylak_id: int | None
    month: int
    mean_area: float
    epsilon: float
    lambda_opt: np.ndarray
    pwm_coefficients: np.ndarray
    threshold_high: float
    threshold_low: float
    converged: bool
    objective_value: float


@dataclass(frozen=True)
class PWMExtremeResult:
    """Aggregated PWM extreme quantile result for one lake (all 12 months)."""

    hylak_id: int | None
    month_results: list[PWMExtremeMonthResult]
    labels_df: pd.DataFrame

    @property
    def thresholds_df(self) -> pd.DataFrame:
        """Return a tidy DataFrame of monthly thresholds."""
        return pd.DataFrame(
            [
                {
                    "hylak_id": mr.hylak_id,
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


@dataclass(frozen=True)
class PWMExtremeConfig:
    """Config for PWM extreme quantile estimation."""

    n_pwm: int = 4
    p_low: float = 0.05
    p_high: float = 0.05
    integration_upper: float = 1.0 - 1e-10
    l2_regularization: float = 1e-6
    min_observations_per_month: int = 10


@dataclass(frozen=True)
class PWMExtremeServiceConfig:
    """Config for one-lake PWM extreme execution."""

    pwm_config: PWMExtremeConfig = PWMExtremeConfig()
    min_valid_per_month: int | None = 10
    min_valid_observations: int | None = 120


@dataclass(frozen=True)
class PWMExtremeBatchConfig:
    """Config for DB batch execution and summary output."""

    output_root: Path
    chunk_size: int = 10_000
    limit_id: int | None = None
    workflow_version: str = CURRENT_PWM_EXTREME_WORKFLOW_VERSION
    pwm_config: PWMExtremeConfig = PWMExtremeConfig()
    min_valid_per_month: int | None = None
    min_valid_observations: int | None = None
    build_summary_cache: bool = True
    plot_summary: bool = True

    def __post_init__(self) -> None:
        normalized_workflow_version = self.workflow_version.strip()
        if not normalized_workflow_version:
            raise ValueError("workflow_version must not be empty")
        object.__setattr__(self, "workflow_version", normalized_workflow_version)

    @property
    def service_config(self) -> PWMExtremeServiceConfig:
        """Service-level config used for each lake in batch mode."""
        return PWMExtremeServiceConfig(
            pwm_config=self.pwm_config,
            min_valid_per_month=self.min_valid_per_month,
            min_valid_observations=self.min_valid_observations,
        )


RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"