"""Visualisation helpers for PWM extreme quantile results."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from .diagnostics import quantile_function_curve


def plot_quantile_functions(
    month_results: list,
    output_dir: Path | None = None,
) -> dict[int, Path]:
    """Plot fitted quantile functions for each month.

    Args:
        month_results: List of PWMExtremeMonthResult objects.
        output_dir: Directory to save figures (None = no saving).

    Returns:
        Dict mapping month → figure path (empty if output_dir is None).
    """
    paths: dict[int, Path] = {}
    for mr in month_results:
        curve_df = quantile_function_curve(mr.lambda_opt, mr.epsilon)
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(curve_df["u"], curve_df["prior_y"], label="Prior (shifted exp)", linestyle="--")
        ax.plot(curve_df["u"], curve_df["fitted_x"], label="Cross-entropy fit")
        ax.set_xlabel("Probability level u")
        ax.set_ylabel("Normalised quantile")
        ax.set_title(f"Month {mr.month}: PWM+CrossEnt quantile function")
        ax.legend()
        fig.tight_layout()
        if output_dir is not None:
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / f"month_{mr.month}_quantile.png"
            fig.savefig(path, dpi=150)
            paths[mr.month] = path
        plt.close(fig)
    return paths


def plot_threshold_summary(
    result,
    output_dir: Path | None = None,
) -> Path | None:
    """Plot monthly threshold summary (high/low thresholds vs mean area).

    Args:
        result: PWMExtremeResult object.
        output_dir: Directory to save figure.

    Returns:
        Path to saved figure, or None.
    """
    thresholds_df = result.thresholds_df
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(thresholds_df["month"], thresholds_df["mean_area"], "o-", label="Mean area")
    ax.plot(thresholds_df["month"], thresholds_df["threshold_high"], "s--", label="High threshold")
    ax.plot(thresholds_df["month"], thresholds_df["threshold_low"], "s--", label="Low threshold")
    ax.set_xlabel("Month")
    ax.set_ylabel("Area")
    ax.set_title(f"PWM+CrossEnt thresholds (hylak_id={result.hylak_id})")
    ax.legend()
    ax.set_xticks(range(1, 13))
    fig.tight_layout()
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "threshold_summary.png"
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return path
    plt.close(fig)
    return None