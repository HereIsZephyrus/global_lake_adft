"""Plot helpers for Hawkes fit diagnostics.

Adapter layer: converts domain types to DataFrames, then delegates to
lakeviz primitives for rendering.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.primitives import plot_bar
from .types import HawkesFitResult, LRTResult, TYPE_LABELS


def plot_event_timeline(events_table: pd.DataFrame) -> plt.Figure:
    """Plot dry/wet event times on a shared time axis."""
    fig, ax = plt.subplots(figsize=(12, 2.8))
    if not events_table.empty:
        dry = events_table[events_table["event_label"] == TYPE_LABELS[0]]
        wet = events_table[events_table["event_label"] == TYPE_LABELS[1]]
        if not dry.empty:
            ax.scatter(dry["time"], np.full(len(dry), 0.0), marker="v", s=28, label="Dry")
        if not wet.empty:
            ax.scatter(wet["time"], np.full(len(wet), 1.0), marker="^", s=28, label="Wet")
    ax.set_yticks([0, 1], ["Dry", "Wet"])
    ax.set_xlabel("Time (years)")
    ax.set_title("Hawkes Event Timeline")
    ax.grid(alpha=0.2, linestyle=":")
    ax.legend(loc="upper right")
    fig.tight_layout()
    return fig


def plot_intensity_decomposition(decomposition_df: pd.DataFrame) -> plt.Figure:
    """Plot lambda decomposition for dry and wet channels."""
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    channels = ("D", "W")
    for axis, channel in zip(axes, channels, strict=True):
        axis.plot(decomposition_df["time"], decomposition_df[f"lambda_{channel}"], label=f"lambda_{channel}")
        axis.plot(
            decomposition_df["time"],
            decomposition_df[f"mu_{channel}"],
            linestyle="--",
            label=f"mu_{channel}",
        )
        axis.plot(
            decomposition_df["time"],
            decomposition_df[f"self_{channel}"],
            linestyle=":",
            label=f"self_{channel}",
        )
        axis.plot(
            decomposition_df["time"],
            decomposition_df[f"cross_{channel}"],
            linestyle="-.",
            label=f"cross_{channel}",
        )
        axis.set_ylabel(channel)
        axis.grid(alpha=0.2, linestyle=":")
        axis.legend(loc="upper right")
    axes[-1].set_xlabel("Time (years)")
    axes[0].set_title("Intensity Decomposition")
    fig.tight_layout()
    return fig


def plot_kernel_matrix(fit_result: HawkesFitResult) -> plt.Figure:
    """Plot kernel amplitude matrix alpha and decay matrix beta."""
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(10, 4))
    alpha = fit_result.alpha
    beta = fit_result.beta
    labels = [f"{source}->{target}" for target in TYPE_LABELS for source in TYPE_LABELS]

    plot_bar(labels, alpha.reshape(-1), ax=ax_left, title="Alpha Matrix", ylabel="alpha", x_rotation=45)
    plot_bar(labels, beta.reshape(-1), ax=ax_right, title="Beta Matrix", ylabel="beta", x_rotation=45)
    ax_left.grid(alpha=0.2, linestyle=":")
    ax_right.grid(alpha=0.2, linestyle=":")
    fig.tight_layout()
    return fig


def plot_lrt_summary(lrt_results: list[LRTResult] | pd.DataFrame) -> plt.Figure:
    """Plot p-values and LR statistics for model-comparison tests."""
    if isinstance(lrt_results, list):
        frame = pd.DataFrame(
            [
                {
                    "test_name": item.test_name,
                    "lr_statistic": item.lr_statistic,
                    "p_value": item.p_value,
                    "significance_level": item.significance_level,
                }
                for item in lrt_results
            ]
        )
    else:
        frame = lrt_results.copy()

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11, 4))
    plot_bar(frame["test_name"], frame["lr_statistic"], ax=ax_left, title="LR Statistic", x_rotation=20)
    ax_left.grid(alpha=0.2, linestyle=":")

    plot_bar(frame["test_name"], frame["p_value"], ax=ax_right, title="P-value", x_rotation=20)
    alpha = (
        float(frame["significance_level"].iloc[0])
        if "significance_level" in frame.columns and not frame.empty
        else 0.05
    )
    ax_right.axhline(alpha, linestyle="--", color="tomato", label=f"alpha={alpha:.2f}")
    ax_right.grid(alpha=0.2, linestyle=":")
    ax_right.legend(loc="upper right")
    fig.tight_layout()
    return fig
