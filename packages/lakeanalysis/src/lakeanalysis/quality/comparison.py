"""Comparison metrics between remote-sensing area and atlas reference area."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class AgreementConfig:
    """Thresholds for classifying agreement between rs_area and atlas_area.

    Attributes:
        excellent: Ratio within [1-t, 1+t] (default ±10%).
        good: Ratio within [1/g, g] (default 2x).
        moderate: Ratio within [1/m, m] (default 5x).
        poor: Ratio within [1/p, p] (default 10x).
    """

    excellent: float = 0.1
    good: float = 2.0
    moderate: float = 5.0
    poor: float = 10.0


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


def classify_agreement(
    ratio: _ArrayLike,
    config: AgreementConfig = AgreementConfig(),
) -> pd.Categorical:
    """Classify each lake's area ratio into agreement levels.

    Levels (in order): excellent, good, moderate, poor, extreme.
    """
    r = np.asarray(ratio, dtype=float)
    labels = pd.CategoricalDtype(
        categories=["excellent", "good", "moderate", "poor", "extreme"],
        ordered=True,
    )
    result = np.full(len(r), "extreme", dtype=object)
    result[np.isnan(r)] = "extreme"

    valid = ~np.isnan(r)
    t = config.excellent
    mask = valid & (r >= 1 - t) & (r <= 1 + t)
    result[mask] = "excellent"

    g = config.good
    mask = valid & (r >= 1 / g) & (r <= g) & (result == "extreme")
    result[mask] = "good"

    m = config.moderate
    mask = valid & (r >= 1 / m) & (r <= m) & (result == "extreme")
    result[mask] = "moderate"

    p = config.poor
    mask = valid & (r >= 1 / p) & (r <= p) & (result == "extreme")
    result[mask] = "poor"

    return pd.Categorical(result, dtype=labels)


def _percentile_stats(
    ratio_clean: np.ndarray,
    log2_clean: np.ndarray,
) -> dict[str, float]:
    """Compute percentile and distribution stats from clean ratio arrays."""
    stats: dict[str, float] = {}
    if len(ratio_clean) > 0:
        pcts = np.percentile(ratio_clean, [5, 25, 50, 75, 95])
        stats["median_ratio"] = float(pcts[2])
        stats["p05_ratio"] = float(pcts[0])
        stats["p25_ratio"] = float(pcts[1])
        stats["p50_ratio"] = float(pcts[2])
        stats["p75_ratio"] = float(pcts[3])
        stats["p95_ratio"] = float(pcts[4])
    else:
        for k in ("median_ratio", "p05_ratio", "p25_ratio",
                   "p50_ratio", "p75_ratio", "p95_ratio"):
            stats[k] = np.nan

    if len(ratio_clean) >= 2:
        q75, q25 = np.percentile(ratio_clean, [75, 25])
        stats["iqr_ratio"] = float(q75 - q25)
    else:
        stats["iqr_ratio"] = np.nan

    if len(log2_clean) > 0:
        stats["mean_log2_ratio"] = float(np.mean(log2_clean))
    else:
        stats["mean_log2_ratio"] = np.nan

    if len(log2_clean) > 1:
        stats["std_log2_ratio"] = float(np.std(log2_clean))
    else:
        stats["std_log2_ratio"] = np.nan

    return stats


def _agreement_counts(
    agreement: pd.Categorical,
) -> dict[str, int]:
    """Count lakes per agreement level."""
    counts = agreement.value_counts()
    return {cat: int(counts.get(cat, 0)) for cat in agreement.categories}


def _direction_counts(
    ratio_clean: np.ndarray,
    threshold: float,
) -> tuple[int, int, int]:
    """Count overestimate, underestimate, and agree lakes."""
    n_over = int(np.sum(ratio_clean > 1 + threshold))
    n_under = int(np.sum(ratio_clean < 1 - threshold))
    n_agree = int(np.sum(
        (ratio_clean >= 1 - threshold) & (ratio_clean <= 1 + threshold)
    ))
    return n_over, n_under, n_agree


def summarize_comparison(
    df: pd.DataFrame,
    *,
    rs_col: str = "rs_area_median",
    atlas_col: str = "atlas_area",
    config: AgreementConfig = AgreementConfig(),
) -> dict[str, float | int | dict[str, int]]:
    """Compute summary statistics for rs_area vs atlas_area comparison.

    Args:
        df: DataFrame with rs_area and atlas_area columns.
        rs_col: Column name for remote-sensing area.
        atlas_col: Column name for atlas reference area.
        config: Agreement classification thresholds.

    Returns:
        Dict with summary statistics.
    """
    valid = df[[rs_col, atlas_col]].dropna()
    valid = valid[valid[atlas_col] > 0]
    n_total = len(valid)
    if n_total == 0:
        return {"n_total": 0}

    ratio = compute_area_ratio(valid[rs_col].values, valid[atlas_col].values)
    log2_r = compute_log2_ratio(valid[rs_col].values, valid[atlas_col].values)
    agreement = classify_agreement(ratio, config)

    ratio_clean = ratio[~np.isnan(ratio)]
    log2_clean = log2_r[~np.isnan(log2_r)]

    n_over, n_under, n_agree = _direction_counts(ratio_clean, config.excellent)

    stats = _percentile_stats(ratio_clean, log2_clean)
    stats["n_total"] = n_total
    stats["n_by_agreement"] = _agreement_counts(agreement)
    stats["n_overestimate"] = n_over
    stats["n_underestimate"] = n_under
    stats["n_agree"] = n_agree
    return stats


def enrich_comparison_df(
    df: pd.DataFrame,
    *,
    rs_mean_col: str = "rs_area_mean",
    rs_median_col: str = "rs_area_median",
    atlas_col: str = "atlas_area",
    config: AgreementConfig = AgreementConfig(),
) -> pd.DataFrame:
    """Add comparison columns to a DataFrame with area_quality data.

    Adds: ratio_mean, ratio_median, rel_diff_mean, rel_diff_median,
          log2_ratio_mean, log2_ratio_median, agreement_mean, agreement_median.
    """
    out = df.copy()

    for rs_col, suffix in [(rs_mean_col, "mean"), (rs_median_col, "median")]:
        out[f"ratio_{suffix}"] = compute_area_ratio(
            out[rs_col], out[atlas_col],
        )
        out[f"rel_diff_{suffix}"] = compute_relative_diff(
            out[rs_col], out[atlas_col],
        )
        out[f"log2_ratio_{suffix}"] = compute_log2_ratio(
            out[rs_col], out[atlas_col],
        )
        out[f"agreement_{suffix}"] = classify_agreement(
            out[f"ratio_{suffix}"], config,
        )

    return out
