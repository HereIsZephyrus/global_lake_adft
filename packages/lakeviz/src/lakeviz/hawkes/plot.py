"""Hawkes process fit diagnostics plots.

All functions accept only plain Python types and pandas DataFrames —
no lakeanalysis domain types are required.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

TYPE_LABELS = ("D", "W")


def plot_event_timeline(events_table: pd.DataFrame) -> plt.Figure:
    """Plot dry/wet event times on a shared time axis.

    Parameters
    ----------
    events_table: columns [event_label, time]
    """
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
    """Plot lambda decomposition for dry and wet channels.

    Parameters
    ----------
    decomposition_df: columns [time, lambda_D, lambda_W, mu_D, mu_W, self_D, self_W, cross_D, cross_W]
    """
    fig, axes = plt.subplots(2, 1, figsize=(12, 6), sharex=True)
    channels = ("D", "W")
    for axis, channel in zip(axes, channels, strict=True):
        axis.plot(decomposition_df["time"], decomposition_df[f"lambda_{channel}"], label=f"lambda_{channel}")
        axis.plot(decomposition_df["time"], decomposition_df[f"mu_{channel}"], linestyle="--", label=f"mu_{channel}")
        axis.plot(decomposition_df["time"], decomposition_df[f"self_{channel}"], linestyle=":", label=f"self_{channel}")
        axis.plot(decomposition_df["time"], decomposition_df[f"cross_{channel}"], linestyle="-.", label=f"cross_{channel}")
        axis.set_ylabel(channel)
        axis.grid(alpha=0.2, linestyle=":")
        axis.legend(loc="upper right")
    axes[-1].set_xlabel("Time (years)")
    axes[0].set_title("Intensity Decomposition")
    fig.tight_layout()
    return fig


def plot_kernel_matrix(alpha: np.ndarray, beta: np.ndarray) -> plt.Figure:
    """Plot kernel amplitude matrix alpha and decay matrix beta.

    Parameters
    ----------
    alpha: 2x2 numpy array (kernel amplitudes).
    beta: 2x2 numpy array (kernel decays).
    """
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(10, 4))
    labels = [f"{source}->{target}" for target in TYPE_LABELS for source in TYPE_LABELS]
    ax_left.bar(labels, alpha.reshape(-1))
    ax_left.set_title("Alpha Matrix")
    ax_left.set_ylabel("alpha")
    ax_left.tick_params(axis="x", rotation=45)
    ax_left.grid(alpha=0.2, linestyle=":")
    ax_right.bar(labels, beta.reshape(-1))
    ax_right.set_title("Beta Matrix")
    ax_right.set_ylabel("beta")
    ax_right.tick_params(axis="x", rotation=45)
    ax_right.grid(alpha=0.2, linestyle=":")
    fig.tight_layout()
    return fig


def plot_lrt_summary(lrt_df: pd.DataFrame) -> plt.Figure:
    """Plot p-values and LR statistics for model-comparison tests.

    Parameters
    ----------
    lrt_df: columns [test_name, lr_statistic, p_value, significance_level]
    """
    frame = lrt_df.copy()
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(11, 4))
    ax_left.bar(frame["test_name"], frame["lr_statistic"])
    ax_left.set_title("LR Statistic")
    ax_left.tick_params(axis="x", rotation=20)
    ax_left.grid(alpha=0.2, linestyle=":")
    ax_right.bar(frame["test_name"], frame["p_value"])
    ax_right.set_title("P-value")
    ax_right.tick_params(axis="x", rotation=20)
    alpha_val = float(frame["significance_level"].iloc[0]) if "significance_level" in frame.columns and not frame.empty else 0.05
    ax_right.axhline(alpha_val, linestyle="--", color="tomato", label=f"alpha={alpha_val:.2f}")
    ax_right.grid(alpha=0.2, linestyle=":")
    ax_right.legend(loc="upper right")
    fig.tight_layout()
    return fig
