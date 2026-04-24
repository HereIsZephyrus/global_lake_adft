"""Application service for one-lake monthly transition execution."""

from __future__ import annotations

import pandas as pd

from .compute import run_monthly_anomaly_transition
from lakesource.quantile.schema import QuantileResult, QuantileServiceConfig


def run_single_lake_service(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    config: QuantileServiceConfig | None = None,
    frozen_year_months: set[int] | None = None,
    use_frozen_mask: bool = True,
) -> QuantileResult:
    """Run one lake through the shared monthly transition workflow service."""
    cfg = config or QuantileServiceConfig()
    applied_frozen = frozen_year_months if use_frozen_mask else None
    return run_monthly_anomaly_transition(
        series_df,
        hylak_id=hylak_id,
        frozen_year_months=applied_frozen,
        min_valid_per_month=cfg.min_valid_per_month,
        min_valid_observations=cfg.min_valid_observations,
    )


def run_quantile_service(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    config: QuantileServiceConfig | None = None,
) -> QuantileResult:
    """Backward-compatible alias for the shared one-lake service."""
    return run_single_lake_service(series_df, hylak_id=hylak_id, config=config)
