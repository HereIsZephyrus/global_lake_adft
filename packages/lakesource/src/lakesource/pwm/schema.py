"""Shared data types and constants for the PWM extreme quantile workflow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from lakeanalysis.extreme.models import ExtremeResult, PWMDiagnostics


@dataclass(frozen=True)
class PWMExtremeMonthResult:
    """PWM extreme quantile result for one lake-month.

    .. note::
        ``mean_area`` stores different values depending on the computation path:

        * Legacy ``compute_monthly_thresholds`` (raw water_area):
          ``mean_area`` = mean of water_area for the given month (km²).

        * New ``compute_pooled_pwm_thresholds`` (STL decomposition):
          ``mean_area`` is set to 0.0 — the threshold scale factor is
          mean(index_value), which is already embedded in the stored
          ``threshold_high`` / ``threshold_low`` columns.
          Downstream consumers should use those columns directly.
    """

    hylak_id: int | None
    month: int
    threshold_quantile: float
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
    """Aggregated PWM extreme quantile result for one lake (all 12 months).

    ``extreme`` holds the shared extreme-event result (labels, events, transitions).
    ``diagnostics`` holds PWM-specific month-level threshold diagnostics.
    """

    extreme: ExtremeResult
    diagnostics: PWMDiagnostics

    @property
    def hylak_id(self) -> int | None:
        return self.extreme.hylak_id

    @property
    def month_results(self) -> list[PWMExtremeMonthResult]:
        return self.diagnostics.month_results

    @property
    def labels_df(self) -> pd.DataFrame:
        return self.extreme.labels_df

    @property
    def extremes_df(self) -> pd.DataFrame:
        return self.extreme.extremes_df

    @property
    def transitions_df(self) -> pd.DataFrame:
        return self.extreme.transitions_df

    @property
    def thresholds_df(self) -> pd.DataFrame:
        return self.diagnostics.thresholds_df

    @property
    def threshold_quantile(self) -> float | None:
        month_results = self.month_results
        if not month_results:
            return None
        return float(month_results[0].threshold_quantile)


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
    method: str = "stl"


@dataclass(frozen=True)
class PWMExtremeBatchConfig:
    """Config for DB batch execution and summary output."""

    output_root: Path
    chunk_size: int = 10_000
    limit_id: int | None = None
    pwm_config: PWMExtremeConfig = PWMExtremeConfig()
    min_valid_per_month: int | None = None
    min_valid_observations: int | None = None
    method: str = "stl"
    build_summary_cache: bool = True
    plot_summary: bool = True

    @property
    def service_config(self) -> PWMExtremeServiceConfig:
        """Service-level config used for each lake in batch mode."""
        return PWMExtremeServiceConfig(
            pwm_config=self.pwm_config,
            min_valid_per_month=self.min_valid_per_month,
            min_valid_observations=self.min_valid_observations,
            method=self.method,
        )


RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"
