"""Comparison metrics between remote-sensing area and atlas reference area."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .metrics import (
    AgreementConfig,
    classify_agreement,
    compute_area_ratio,
    compute_log2_ratio,
    compute_relative_diff,
)


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
    config: AgreementConfig | None = None,
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
    if config is None:
        config = AgreementConfig()
    valid = df[[rs_col, atlas_col]].dropna()
    valid = valid[valid[atlas_col] > 0]
    n_total = len(valid)
    if n_total == 0:
        return {"n_total": 0}

    ratio = compute_area_ratio(valid[rs_col].to_numpy(), valid[atlas_col].to_numpy())
    log2_r = compute_log2_ratio(valid[rs_col].to_numpy(), valid[atlas_col].to_numpy())
    agreement = classify_agreement(ratio, config)

    ratio_clean = ratio[~np.isnan(ratio)]
    log2_clean = log2_r[~np.isnan(log2_r)]

    n_over, n_under, n_agree = _direction_counts(ratio_clean, config.good)

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
    config: AgreementConfig | None = None,
) -> pd.DataFrame:
    """Add comparison columns to a DataFrame with area_quality data.

    Adds: ratio_mean, ratio_median, rel_diff_mean, rel_diff_median,
          log2_ratio_mean, log2_ratio_median, agreement_mean, agreement_median.
    """
    if config is None:
        config = AgreementConfig()
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
