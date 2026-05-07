"""Matplotlib rendering for PWM extreme quantile results.

Generic interfaces: accepts only DataFrame/ndarray/scalar types.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.config import DEFAULT_VIZ_CONFIG
from lakeviz.domain.pwm_extreme import (
    plot_pwm_extreme_quantile_function,
    plot_pwm_extreme_threshold_summary as _plot_pwm_extreme_threshold_summary,
)


def _render_quantile_function(curve_df: pd.DataFrame, month: int, output_dir: Path | None = None) -> Path | None:
    fig = plot_pwm_extreme_quantile_function(curve_df, month)
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
    fig = _plot_pwm_extreme_threshold_summary(thresholds_df, hylak_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "threshold_summary.png"
    fig.savefig(path, dpi=DEFAULT_VIZ_CONFIG.default_dpi)
    plt.close(fig)
    return path
