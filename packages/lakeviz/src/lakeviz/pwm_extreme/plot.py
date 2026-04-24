"""Plotting functions for PWM extreme quantile results.

Thin wrappers around lakeanalysis.pwm_extreme.plot that standardise
output directories and figure formats for the viz layer.
"""

from __future__ import annotations

from pathlib import Path

from lakeanalysis.pwm_extreme.plot import (
    plot_quantile_functions,
    plot_threshold_summary,
)


def plot_pwm_extreme_quantile_functions(
    result,
    output_dir: Path,
) -> dict[int, Path]:
    """Save per-month quantile function plots."""
    return plot_quantile_functions(result.month_results, output_dir=output_dir)


def plot_pwm_extreme_threshold_summary(
    result,
    output_dir: Path,
) -> Path | None:
    """Save threshold summary plot."""
    return plot_threshold_summary(result, output_dir=output_dir)
