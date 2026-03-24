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
