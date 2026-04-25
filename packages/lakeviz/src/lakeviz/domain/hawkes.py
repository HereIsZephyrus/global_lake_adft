"""Domain-level draw functions — Hawkes process diagnostics."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.bar import draw_bar
from lakeviz.draw.reference import draw_axhline
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.reference import ReferenceLineStyle

TYPE_LABELS = ("D", "W")


def draw_event_timeline(
    ax: plt.Axes,
    events_table: pd.DataFrame,
    *,
    dry_style: ScatterStyle = ScatterStyle(marker="v", s=28, label="Dry"),
    wet_style: ScatterStyle = ScatterStyle(marker="^", s=28, label="Wet"),
    axis_style: AxisStyle = AxisStyle(xlabel="Time (years)", title="Hawkes Event Timeline"),
) -> None:
    if not events_table.empty:
        dry = events_table[events_table["event_label"] == TYPE_LABELS[0]]
        wet = events_table[events_table["event_label"] == TYPE_LABELS[1]]
        if not dry.empty:
            draw_scatter(ax, dry["time"], np.full(len(dry), 0.0), style=dry_style)
        if not wet.empty:
            draw_scatter(ax, wet["time"], np.full(len(wet), 1.0), style=wet_style)
    ax.set_yticks([0, 1], ["Dry", "Wet"])
    apply_axis_style(ax, axis_style._replace(grid_alpha=0.2, grid_linestyle=":"))
    ax.legend(loc="upper right")


def draw_intensity_decomposition(
    ax_d: plt.Axes,
    ax_w: plt.Axes,
    decomposition_df: pd.DataFrame,
    *,
    axis_style: AxisStyle = AxisStyle(title="Intensity Decomposition"),
) -> None:
    channels = ("D", "W")
    line_styles = {
        "lambda": LineStyle(),
        "mu": LineStyle(linestyle="--"),
        "self": LineStyle(linestyle=":"),
        "cross": LineStyle(linestyle="-."),
    }
    for axis, channel in zip((ax_d, ax_w), channels, strict=True):
        for prefix, style in line_styles.items():
            col = f"{prefix}_{channel}"
            draw_line(axis, decomposition_df["time"], decomposition_df[col], style=style._replace(label=col))
        axis.set_ylabel(channel)
        axis.grid(alpha=0.2, linestyle=":")
        axis.legend(loc="upper right")
    ax_w.set_xlabel("Time (years)")
    ax_d.set_title(axis_style.title)


def draw_kernel_matrix(
    ax_alpha: plt.Axes,
    ax_beta: plt.Axes,
    alpha: np.ndarray,
    beta: np.ndarray,
    *,
    alpha_axis: AxisStyle = AxisStyle(title="Alpha Matrix", ylabel="alpha", x_rotation=45),
    beta_axis: AxisStyle = AxisStyle(title="Beta Matrix", ylabel="beta", x_rotation=45),
) -> None:
    labels = [f"{source}->{target}" for target in TYPE_LABELS for source in TYPE_LABELS]
    draw_bar(ax_alpha, labels, alpha.reshape(-1), style=BarStyle())
    apply_axis_style(ax_alpha, alpha_axis._replace(grid_alpha=0.2, grid_linestyle=":"))
    draw_bar(ax_beta, labels, beta.reshape(-1), style=BarStyle())
    apply_axis_style(ax_beta, beta_axis._replace(grid_alpha=0.2, grid_linestyle=":"))


def draw_lrt_summary(
    ax_lr: plt.Axes,
    ax_p: plt.Axes,
    lrt_df: pd.DataFrame,
    *,
    lr_axis: AxisStyle = AxisStyle(title="LR Statistic", x_rotation=20),
    p_axis: AxisStyle = AxisStyle(title="P-value", x_rotation=20),
) -> None:
    frame = lrt_df.copy()
    draw_bar(ax_lr, frame["test_name"], frame["lr_statistic"], style=BarStyle())
    apply_axis_style(ax_lr, lr_axis._replace(grid_alpha=0.2, grid_linestyle=":"))
    draw_bar(ax_p, frame["test_name"], frame["p_value"], style=BarStyle())
    alpha_val = float(frame["significance_level"].iloc[0]) if "significance_level" in frame.columns and not frame.empty else 0.05
    draw_axhline(ax_p, alpha_val, style=ReferenceLineStyle(linestyle="--", color="tomato", label=f"alpha={alpha_val:.2f}"))
    apply_axis_style(ax_p, p_axis._replace(grid_alpha=0.2, grid_linestyle=":"))
    ax_p.legend(loc="upper right")


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_event_timeline(events_table: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 2.8))
    draw_event_timeline(ax, events_table)
    fig.tight_layout()
    return fig


def plot_intensity_decomposition(decomposition_df: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    draw_intensity_decomposition(axes[0], axes[1], decomposition_df)
    fig.tight_layout()
    return fig


def plot_kernel_matrix(alpha: np.ndarray, beta: np.ndarray) -> plt.Figure:
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(10, 4))
    draw_kernel_matrix(ax_left, ax_right, alpha, beta)
    fig.tight_layout()
    return fig


def plot_lrt_summary(lrt_df: pd.DataFrame) -> plt.Figure:
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11, 4))
    draw_lrt_summary(ax_left, ax_right, lrt_df)
    fig.tight_layout()
    return fig