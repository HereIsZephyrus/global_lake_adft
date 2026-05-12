"""Application service for one-lake PWM extreme quantile execution."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.quality.frozen import filter_frozen_rows
from lakeanalysis.quantile.compute import validate_monthly_series
from lakeanalysis.decomposition.base import DecompositionMethod
from lakeanalysis.decomposition.monthly_climatology import MonthlyClimatologyMethod
from lakeanalysis.decomposition.stl_percentile import STLPercentileMethod

from .compute import compute_pooled_pwm_thresholds
from lakesource.pwm_extreme.schema import (
    PWMExtremeResult,
    PWMExtremeServiceConfig,
)


def _create_method(method: str | None) -> DecompositionMethod:
    if method is None or method == "stl":
        return STLPercentileMethod()
    if method == "legacy":
        return MonthlyClimatologyMethod()
    raise ValueError(f"Unknown decomposition method: {method!r}")


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

    method = _create_method(cfg.method)

    valid_df = validate_monthly_series(series_df)

    if frozen_year_months and use_frozen_mask:
        valid_df = filter_frozen_rows(valid_df, frozen_year_months)

    if valid_df.empty:
        raise ValueError("No valid observations remain after filtering")

    result = method.decompose(valid_df)

    return compute_pooled_pwm_thresholds(
        result,
        hylak_id=hylak_id,
        config=cfg.pwm_config,
    )
