"""Pure computation functions for lake area quality metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd


def compute_median_area(df: pd.DataFrame) -> float:
    """Compute the median of water_area for a single lake."""
    return float(df["water_area"].median())


def compute_mean_area(df: pd.DataFrame) -> float:
    """Compute the mean of water_area for a single lake."""
    return float(df["water_area"].mean())


def compute_quantile_area(df: pd.DataFrame, quantile: float = 0.75) -> float:
    """Compute the given quantile of water_area for a single lake."""
    return float(df["water_area"].quantile(quantile))


def is_anomalous(rs_area_median: float) -> bool:
    """Return True when a lake's median area is zero (anomalous)."""
    return rs_area_median == 0.0


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


def compute_penalized_volatility(
    values: pd.Series,
) -> dict[str, float]:
    """Compute H×CV (entropy-weighted coefficient of variation) from a defrozen value series.

    Replaces the old std_pct_change / sqrt(n_zero_delta) metric with:
      - H:   discrete Shannon entropy, H = -sum(p_i * log2(p_i))
      - CV:  std(water_area) / mean(water_area)
      - H×CV: entropy-weighted coefficient of variation

    Args:
        values: water_area series with frozen months already removed,
            sorted chronologically.

    Returns keys:
      - n_obs: number of observations
      - n_distinct: number of distinct values
      - dominant_ratio: frequency of most common value / n_obs
      - cv: coefficient of variation (std / mean)
      - H: Shannon entropy in bits
      - h_cv: H × CV
      - penalized_volatility: alias for h_cv (backward compat)
    """
    values = pd.to_numeric(values, errors="coerce").dropna().reset_index(drop=True)
    n_obs = len(values)
    if n_obs < 2:
        return {
            "n_obs": float(n_obs),
            "n_distinct": float(n_obs),
            "dominant_ratio": 1.0 if n_obs == 1 else 0.0,
            "cv": None,
            "H": None,
            "h_cv": None,
            "penalized_volatility": None,
        }

    vc = values.value_counts(dropna=False)
    n_distinct = len(vc)
    dominant_ratio = float(vc.iloc[0]) / float(n_obs)

    p = vc.values / n_obs
    H = float(-np.sum(p * np.log2(p)))

    mean_a = float(values.mean())
    std_a = float(values.std())
    cv = std_a / mean_a if mean_a > 0 else None
    h_cv = H * cv if cv is not None else None

    return {
        "n_obs": float(n_obs),
        "n_distinct": float(n_distinct),
        "dominant_ratio": dominant_ratio,
        "cv": cv,
        "H": H,
        "h_cv": h_cv,
        "penalized_volatility": h_cv,
    }


def compute_area_range(
    df: pd.DataFrame,
    value_column: str = "water_area",
) -> dict[str, float]:
    """Compute the min and max of water_area for a single lake.

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
    """Classify whether atlas_area falls outside the observed time-series range."""
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


_ArrayLike = pd.Series | np.ndarray


def compute_area_ratio(
    rs_area: _ArrayLike,
    atlas_area: _ArrayLike,
) -> np.ndarray:
    """Compute rs_area / atlas_area, returning NaN where atlas_area <= 0."""
    rs = np.asarray(rs_area, dtype=float)
    at = np.asarray(atlas_area, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        ratio = np.where(at > 0, rs / at, np.nan)
    return ratio


def compute_relative_diff(
    rs_area: _ArrayLike,
    atlas_area: _ArrayLike,
) -> np.ndarray:
    """Compute (rs_area - atlas_area) / atlas_area, NaN where atlas_area <= 0."""
    ratio = compute_area_ratio(rs_area, atlas_area)
    return ratio - 1.0


def compute_log2_ratio(
    rs_area: _ArrayLike,
    atlas_area: _ArrayLike,
) -> np.ndarray:
    """Compute log2(rs_area / atlas_area), NaN where ratio <= 0."""
    ratio = compute_area_ratio(rs_area, atlas_area)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(ratio > 0, np.log2(ratio), np.nan)


class AgreementConfig:
    """Thresholds for classifying agreement between rs_area and atlas_area.

    Attributes:
        good: Ratio within [1/g, g] (default 2x).
        moderate: Ratio within [1/m, m] (default 5x).
        poor: Ratio within [1/p, p] (default 10x).
    """

    __slots__ = ("good", "moderate", "poor")

    def __init__(
        self,
        good: float = 2.0,
        moderate: float = 5.0,
        poor: float = 10.0,
    ) -> None:
        self.good = good
        self.moderate = moderate
        self.poor = poor

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AgreementConfig):
            return NotImplemented
        return (
            self.good == other.good
            and self.moderate == other.moderate
            and self.poor == other.poor
        )

    def __hash__(self) -> int:
        return hash((self.good, self.moderate, self.poor))

    def __repr__(self) -> str:
        return (
            f"AgreementConfig(good={self.good}, "
            f"moderate={self.moderate}, poor={self.poor})"
        )


def classify_agreement(
    ratio: _ArrayLike,
    config: AgreementConfig | None = None,
) -> pd.Categorical:
    """Classify each lake's area ratio into agreement levels.

    Levels (in order): good, moderate, poor.
    """
    if config is None:
        config = AgreementConfig()
    r = np.asarray(ratio, dtype=float)
    labels = pd.CategoricalDtype(
        categories=["good", "moderate", "poor"],
        ordered=True,
    )
    result = np.full(len(r), "poor", dtype=object)
    result[np.isnan(r)] = "poor"

    valid = ~np.isnan(r)

    g = config.good
    mask = valid & (r >= 1 / g) & (r <= g)
    result[mask] = "good"

    m = config.moderate
    mask = valid & (r >= 1 / m) & (r <= m) & (result == "poor")
    result[mask] = "moderate"

    return pd.Categorical(result, dtype=labels)
