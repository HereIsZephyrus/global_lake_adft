"""Area quality metrics for individual lakes."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


def compute_median_area(df: pd.DataFrame) -> float:
    """Compute the median of water_area for a single lake.

    Args:
        df: DataFrame with at least a ``water_area`` column,
            as returned by ``fetch_lake_area_chunk``.

    Returns:
        Median water_area value.
    """
    return float(df["water_area"].median())


def compute_mean_area(df: pd.DataFrame) -> float:
    """Compute the mean of water_area for a single lake.

    Args:
        df: DataFrame with at least a ``water_area`` column,
            as returned by ``fetch_lake_area_chunk``.

    Returns:
        Mean water_area value.
    """
    return float(df["water_area"].mean())


def is_anomalous(rs_area_median: float) -> bool:
    """Return True when a lake's median area is zero (anomalous).

    Args:
        rs_area_median: Median remote-sensing area for the lake.

    Returns:
        True if rs_area_median == 0.
    """
    return rs_area_median == 0.0


@dataclass(frozen=True)
class FlatnessFilterConfig:
    """Config for flat-series filters.

    Attributes:
        dominant_ratio_threshold: Flag when most common value frequency / N is
            greater than or equal to this threshold.
        round_digits: Optional rounding digits for value bucketing before
            computing value frequencies.
    """

    dominant_ratio_threshold: float = 0.8
    round_digits: int | None = None


def _prepare_values(
    df: pd.DataFrame,
    value_column: str = "water_area",
    round_digits: int | None = None,
) -> pd.Series:
    """Return cleaned value series for flatness metrics."""
    values = pd.to_numeric(df[value_column], errors="coerce").dropna()
    if round_digits is not None:
        values = values.round(round_digits)
    return values


def compute_flatness_metrics(
    df: pd.DataFrame,
    value_column: str = "water_area",
    round_digits: int | None = None,
) -> dict[str, float]:
    """Compute flatness metrics from a single-lake series.

    Returns keys:
      - n_obs
      - n_unique
      - dominant_ratio
      - unique_ratio
    """
    values = _prepare_values(df, value_column=value_column, round_digits=round_digits)
    n_obs = int(values.shape[0])
    if n_obs == 0:
        return {
            "n_obs": 0.0,
            "n_unique": 0.0,
            "dominant_ratio": 0.0,
            "unique_ratio": 0.0,
        }

    value_counts = values.value_counts(dropna=False)
    n_unique = int(value_counts.shape[0])
    dominant_ratio = float(value_counts.iloc[0]) / float(n_obs)
    unique_ratio = float(n_unique) / float(n_obs)
    return {
        "n_obs": float(n_obs),
        "n_unique": float(n_unique),
        "dominant_ratio": dominant_ratio,
        "unique_ratio": unique_ratio,
    }


def compute_area_range(
    df: pd.DataFrame,
    value_column: str = "water_area",
) -> dict[str, float]:
    """Compute the min and max of water_area for a single lake.

    Args:
        df: DataFrame with at least a ``water_area`` column,
            as returned by ``fetch_lake_area_chunk``.
        value_column: Column name for the area values.

    Returns:
        Dict with ``min_area`` and ``max_area`` in the same unit as input.
        Returns ``0.0`` for both when the series is empty.
    """
    values = _prepare_values(df, value_column=value_column)
    if values.empty:
        return {"min_area": 0.0, "max_area": 0.0}
    return {"min_area": float(values.min()), "max_area": float(values.max())}


def classify_outside_range(
    atlas_area: float,
    min_area: float,
    max_area: float,
) -> dict[str, bool | float]:
    """Classify whether atlas_area falls outside the observed time-series range.

    Args:
        atlas_area: Reference area from lake_info (km²).
        min_area: Minimum observed area from time series (km²).
        max_area: Maximum observed area from time series (km²).

    Returns:
        Dict with classification results:
          - is_outside_range: atlas_area is below min or above max.
          - is_below_min: atlas_area < min_area.
          - is_above_max: atlas_area > max_area.
          - atlas_area: Input value echoed back.
          - min_area: Input value echoed back.
          - max_area: Input value echoed back.
    """
    is_below_min = atlas_area < min_area
    is_above_max = atlas_area > max_area

    if atlas_area <= 0:
        is_outside_range = False
        is_below_min = False
        is_above_max = False
    else:
        is_outside_range = is_below_min or is_above_max

    return {
        "is_outside_range": is_outside_range,
        "is_below_min": bool(is_below_min),
        "is_above_max": bool(is_above_max),
        "atlas_area": float(atlas_area),
        "min_area": float(min_area),
        "max_area": float(max_area),
    }


def classify_area_anomaly(
    df: pd.DataFrame,
    rs_area_median: float,
    config: FlatnessFilterConfig,
) -> dict[str, bool | float]:
    """Classify whether a lake should be treated as area anomaly.

    Rules:
      1. median-zero anomaly: rs_area_median == 0
      2. flatness anomaly: dominant-ratio filter triggers
    """
    metrics = compute_flatness_metrics(
        df,
        value_column="water_area",
        round_digits=config.round_digits,
    )
    dominant_ratio = float(metrics["dominant_ratio"])

    is_median_zero = is_anomalous(rs_area_median)
    is_flat_dominant = dominant_ratio >= config.dominant_ratio_threshold
    is_flat = is_flat_dominant

    return {
        "is_anomalous": bool(is_median_zero or is_flat),
        "is_median_zero": bool(is_median_zero),
        "is_flat": bool(is_flat),
        "is_flat_dominant": bool(is_flat_dominant),
        "dominant_ratio": dominant_ratio,
    }
