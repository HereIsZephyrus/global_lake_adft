"""Domain-level draw functions — PWM extreme quantile results."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.presets import (
    PWM_EXTREME_FITTED,
    PWM_EXTREME_HIGH,
    PWM_EXTREME_LOW,
    PWM_EXTREME_MEAN,
    PWM_EXTREME_PRIOR,
)


def draw_quantile_function(
    ax: plt.Axes,
    curve_df: pd.DataFrame,
    month: int,
    *,
    prior_style: LineStyle = PWM_EXTREME_PRIOR,
    fitted_style: LineStyle = PWM_EXTREME_FITTED,
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
    mean_style: LineStyle = PWM_EXTREME_MEAN,
    high_style: LineStyle = PWM_EXTREME_HIGH,
    low_style: LineStyle = PWM_EXTREME_LOW,
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


def plot_pwm_extreme_quantile_function(curve_df: pd.DataFrame, month: int) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(6, 4))
    draw_quantile_function(ax, curve_df, month)
    fig.tight_layout()
    return fig


def plot_pwm_extreme_threshold_summary(thresholds_df: pd.DataFrame, hylak_id: int) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_threshold_summary(ax, thresholds_df, hylak_id)
    fig.tight_layout()
    return fig
