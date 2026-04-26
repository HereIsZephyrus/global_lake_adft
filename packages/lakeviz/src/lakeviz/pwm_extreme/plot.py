"""Matplotlib rendering for PWM extreme quantile results.

Generic interfaces: accepts only DataFrame/ndarray/scalar types.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.config import DEFAULT_VIZ_CONFIG


def _render_quantile_function(
    curve_df: pd.DataFrame,
    month: int,
    output_dir: Path | None = None,
) -> Path | None:
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(curve_df["u"], curve_df["prior_y"], label="Prior (shifted exp)", linestyle="--")
    ax.plot(curve_df["u"], curve_df["fitted_x"], label="Cross-entropy fit")
    ax.set_xlabel("Probability level u")
    ax.set_ylabel("Normalised quantile")
    ax.set_title(f"Month {month}: PWM+CrossEnt quantile function")
    ax.legend()
    fig.tight_layout()
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / f"month_{month}_quantile.png"
        fig.savefig(path, dpi=DEFAULT_VIZ_CONFIG.default_dpi)
        plt.close(fig)
        return path
    plt.close(fig)
    return None


def plot_pwm_extreme_quantile_functions(
    month_curves: list[tuple[int, pd.DataFrame]],
    output_dir: Path,
) -> dict[int, Path]:
    """Render and save per-month quantile function plots.

    Args:
        month_curves: List of (month, curve_df) tuples where curve_df has columns
                      [u, prior_y, fitted_x].
        output_dir: Directory to save figures.

    Returns:
        Dict mapping month → figure path.
    """
    paths: dict[int, Path] = {}
    for month, curve_df in month_curves:
        path = _render_quantile_function(curve_df, month, output_dir)
        if path is not None:
            paths[month] = path
    return paths


def plot_pwm_extreme_threshold_summary(
    thresholds_df: pd.DataFrame,
    hylak_id: int,
    output_dir: Path,
) -> Path:
    """Render and save monthly threshold summary plot.

    Args:
        thresholds_df: DataFrame with columns [month, mean_area, threshold_high, threshold_low].
        hylak_id: Lake ID for title.
        output_dir: Directory to save figure.

    Returns:
        Path to saved figure.
    """
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(thresholds_df["month"], thresholds_df["mean_area"], "o-", label="Mean area")
    ax.plot(thresholds_df["month"], thresholds_df["threshold_high"], "s--", label="High threshold")
    ax.plot(thresholds_df["month"], thresholds_df["threshold_low"], "s--", label="Low threshold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Area")
    ax.set_title(f"PWM+CrossEnt thresholds (hylak_id={hylak_id})")
    ax.legend()
    ax.set_xticks(range(1, 13))
    fig.tight_layout()
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "threshold_summary.png"
    fig.savefig(path, dpi=DEFAULT_VIZ_CONFIG.default_dpi)
    plt.close(fig)
    return path
