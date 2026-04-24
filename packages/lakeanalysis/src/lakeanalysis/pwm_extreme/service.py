"""Application service for one-lake PWM extreme quantile execution."""

from __future__ import annotations

import pandas as pd

from .compute import compute_monthly_thresholds
from lakesource.pwm_extreme.schema import (
    PWMExtremeConfig,
    PWMExtremeResult,
    PWMExtremeServiceConfig,
)


def run_single_lake_service(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    config: PWMExtremeServiceConfig | None = None,
    frozen_year_months: set[int] | None = None,
    use_frozen_mask: bool = True,
) -> PWMExtremeResult:
    """Run one lake through the PWM extreme quantile workflow."""
    cfg = config or PWMExtremeServiceConfig()
    applied_frozen = frozen_year_months if use_frozen_mask else None
    return compute_monthly_thresholds(
        series_df,
        hylak_id=hylak_id,
        config=cfg.pwm_config,
        frozen_year_months=applied_frozen,
    )
