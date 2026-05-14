from __future__ import annotations

import pandas as pd

from lakeanalysis.extreme.service import run_single_lake_service as _run_extreme_service

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
    """Run one lake through the quantile-based monthly transition workflow."""
    cfg = config or QuantileServiceConfig()

    return _run_extreme_service(
        series_df,
        hylak_id=hylak_id,
        method=cfg.method,
        min_valid_per_month=cfg.min_valid_per_month,
        min_valid_observations=cfg.min_valid_observations,
        frozen_year_months=frozen_year_months,
        use_frozen_mask=use_frozen_mask,
        require_min_monthly_observations=True,
        require_min_total_observations=True,
        compute_fn=run_monthly_anomaly_transition,
    )


def run_quantile_service(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    config: QuantileServiceConfig | None = None,
) -> QuantileResult:
    """Backward-compatible alias for the shared one-lake service."""
    return run_single_lake_service(series_df, hylak_id=hylak_id, config=config)
