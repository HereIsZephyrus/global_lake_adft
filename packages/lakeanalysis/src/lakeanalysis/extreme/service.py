"""Shared one-lake service pipeline for extreme-event workflows."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pandas as pd

from lakeanalysis.decomposition.base import DecompositionMethod, DecompositionResult
from lakeanalysis.decomposition.monthly_climatology import MonthlyClimatologyMethod
from lakeanalysis.decomposition.stl_percentile import STLPercentileMethod
from lakeanalysis.quality.frozen import filter_frozen_rows

from .compute import validate_monthly_series


def ensure_min_total_observations(
    index_df: pd.DataFrame,
    min_valid_observations: int | None,
) -> None:
    """Ensure the series has at least the requested total observations."""
    if min_valid_observations is not None and len(index_df) < min_valid_observations:
        raise ValueError("Insufficient valid monthly observations overall")


def ensure_min_monthly_observations(
    index_df: pd.DataFrame,
    min_valid_per_month: int | None,
) -> None:
    """Ensure each calendar month has enough observations when requested."""
    if min_valid_per_month is None:
        return
    month_counts = index_df.groupby("month").size().reindex(range(1, 13), fill_value=0)
    if (month_counts < min_valid_per_month).any():
        raise ValueError("Insufficient valid observations for one or more calendar months")


def create_decomposition_method(method: str | None) -> DecompositionMethod:
    """Return a decomposition method by name."""
    if method is None or method == "stl":
        return STLPercentileMethod()
    if method == "legacy":
        return MonthlyClimatologyMethod()
    raise ValueError(f"Unknown decomposition method: {method!r}")


def run_single_lake_service(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    method: str = "stl",
    min_valid_per_month: int | None = None,
    min_valid_observations: int | None = None,
    frozen_year_months: set[int] | None = None,
    use_frozen_mask: bool = True,
    compute_fn: Callable[[DecompositionResult, Any], Any] | None = None,
    compute_kwargs: dict | None = None,
    require_min_monthly_observations: bool = False,
    require_min_total_observations: bool = False,
) -> Any:
    """Run one lake through the shared extreme-event workflow pipeline.

    Steps:
    1. Validate and normalize the monthly series.
    2. Optionally filter frozen (ice-covered) months.
    3. Run decomposition to produce an anomaly index.
    4. Call algorithm-specific ``compute_fn`` with the decomposition result.
    """
    cfg_method = method
    decomposer = create_decomposition_method(cfg_method)

    valid_df = validate_monthly_series(series_df)

    if frozen_year_months and use_frozen_mask:
        valid_df = filter_frozen_rows(valid_df, frozen_year_months)

    if valid_df.empty:
        raise ValueError("No valid observations remain after filtering")

    result = decomposer.decompose(valid_df)

    if require_min_monthly_observations:
        ensure_min_monthly_observations(result.index_df, min_valid_per_month)
    if require_min_total_observations:
        ensure_min_total_observations(result.index_df, min_valid_observations)

    if compute_fn is not None:
        kwargs = dict(compute_kwargs or {})
        return compute_fn(
            result,
            hylak_id=hylak_id,
            **kwargs,
        )

    return result
