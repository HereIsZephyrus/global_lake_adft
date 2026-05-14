from __future__ import annotations

import pandas as pd

from lakeanalysis.extreme.service import run_single_lake_service as _run_extreme_service

from .compute import compute_pooled_pwm_thresholds
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

    return _run_extreme_service(
        series_df,
        hylak_id=hylak_id,
        method=cfg.method,
        min_valid_per_month=cfg.min_valid_per_month,
        min_valid_observations=cfg.min_valid_observations,
        frozen_year_months=frozen_year_months,
        use_frozen_mask=use_frozen_mask,
        compute_fn=compute_pooled_pwm_thresholds,
        compute_kwargs={"config": cfg.pwm_config},
    )
