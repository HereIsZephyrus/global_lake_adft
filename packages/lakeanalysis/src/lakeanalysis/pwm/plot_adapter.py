"""Plot helpers for PWM extreme quantile results.

Adapter layer: converts lakeanalysis domain types to generic DataFrames,
then delegates to lakeviz.pwm for rendering.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt

from lakeviz.pwm import (
    plot_pwm_extreme_quantile_functions as _plot_quantile_functions,
    plot_pwm_extreme_threshold_summary as _plot_threshold_summary,
)
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
    if output_dir is None:
        return {}
    month_curves = [
        (mr.month, quantile_function_curve(mr.lambda_opt, mr.epsilon))
        for mr in month_results
    ]
    return _plot_quantile_functions(month_curves, output_dir)


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
    if output_dir is None:
        return None
    fig = _plot_threshold_summary(result.thresholds_df, result.hylak_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "threshold_summary.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return out_path
