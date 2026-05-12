"""Application service for one-lake monthly transition execution."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.quality.frozen import filter_frozen_rows
from lakeanalysis.decomposition.base import DecompositionMethod
from lakeanalysis.decomposition.monthly_climatology import MonthlyClimatologyMethod
from lakeanalysis.decomposition.stl_percentile import STLPercentileMethod

from .compute import run_monthly_anomaly_transition, validate_monthly_series
from lakesource.quantile.schema import QuantileResult, QuantileServiceConfig


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
    config: QuantileServiceConfig | None = None,
    frozen_year_months: set[int] | None = None,
    use_frozen_mask: bool = True,
) -> QuantileResult:
    """Run one lake through the shared monthly transition workflow service."""
    cfg = config or QuantileServiceConfig()

    method = _create_method(cfg.method)

    valid_df = validate_monthly_series(series_df)

    if frozen_year_months and use_frozen_mask:
        valid_df = filter_frozen_rows(valid_df, frozen_year_months)

    if valid_df.empty:
        raise ValueError("No valid observations remain after filtering")

    result = method.decompose(valid_df)

    return run_monthly_anomaly_transition(
        result,
        hylak_id=hylak_id,
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
