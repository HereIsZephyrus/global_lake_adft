"""Domain-level draw functions — PWM extreme quantile results."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle


def draw_quantile_function(
    ax: plt.Axes,
    curve_df: pd.DataFrame,
    month: int,
    *,
    prior_style: LineStyle = LineStyle(linestyle="--", label="Prior (shifted exp)"),
    fitted_style: LineStyle = LineStyle(label="Cross-entropy fit"),
    axis_style: AxisStyle | None = None,
) -> None:
    draw_line(ax, curve_df["u"], curve_df["prior_y"], style=prior_style)
    draw_line(ax, curve_df["u"], curve_df["fitted_x"], style=fitted_style)
    if axis_style is None:
        axis_style = AxisStyle(xlabel="Probability level u", ylabel="Normalised quantile", title=f"Month {month}: PWM+CrossEnt quantile function")
    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_threshold_summary(
    ax: plt.Axes,
    thresholds_df: pd.DataFrame,
    hylak_id: int,
    *,
    mean_style: LineStyle = LineStyle(marker="o", label="Mean area"),
    high_style: LineStyle = LineStyle(marker="s", linestyle="--", label="High threshold"),
    low_style: LineStyle = LineStyle(marker="s", linestyle="--", label="Low threshold"),
    axis_style: AxisStyle | None = None,
) -> None:
    draw_line(ax, thresholds_df["month"], thresholds_df["mean_area"], style=mean_style)
    draw_line(ax, thresholds_df["month"], thresholds_df["threshold_high"], style=high_style)
    draw_line(ax, thresholds_df["month"], thresholds_df["threshold_low"], style=low_style)
    if axis_style is None:
        axis_style = AxisStyle(xlabel="Month", ylabel="Area", title=f"PWM+CrossEnt thresholds (hylak_id={hylak_id})")
    apply_axis_style(ax, axis_style)
    ax.legend()
    ax.set_xticks(range(1, 13))


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_pwm_extreme_quantile_functions(month_curves, output_dir) -> dict[int, str]:
    from pathlib import Path
    paths: dict[int, str] = {}
    for month, curve_df in month_curves:
        fig, ax = plt.subplots(figsize=(6, 4))
        draw_quantile_function(ax, curve_df, month)
        fig.tight_layout()
        if output_dir is not None:
            output_dir = Path(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / f"month_{month}_quantile.png"
            fig.savefig(path, dpi=150)
            paths[month] = str(path)
        plt.close(fig)
    return paths


def plot_pwm_extreme_threshold_summary(thresholds_df, hylak_id, output_dir) -> str:
    from pathlib import Path
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_threshold_summary(ax, thresholds_df, hylak_id)
    fig.tight_layout()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "threshold_summary.png"
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return str(path)