"""Matplotlib plots for lake-pair similarity (Pearson and ACF cosine)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

log = logging.getLogger(__name__)


def plot_pearson_distribution(summary_df: pd.DataFrame) -> plt.Figure:
    """Histogram of Pearson correlation across lake pairs.

    Args:
        summary_df: DataFrame with column pearson_r.

    Returns:
        Matplotlib Figure.
    """
    values = summary_df["pearson_r"].dropna()
    fig, ax = plt.subplots(figsize=(8, 5))
    if len(values) == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig
    ax.hist(values, bins=40, density=True, alpha=0.6, edgecolor="white", linewidth=0.4)
    mean_val = float(values.mean())
    ax.axvline(mean_val, color="red", linestyle="--", label=f"平均值 = {mean_val:.3f}")
    ax.set_xlabel("Pearson r")
    ax.set_ylabel("密度")
    ax.set_title("Pearson相关系数分布")
    ax.legend()
    ax.set_xlim(-1.05, 1.05)
    fig.tight_layout()
    log.debug("plot_pearson_distribution: n=%d", len(values))
    return fig


def plot_acf_cosine_distribution(summary_df: pd.DataFrame) -> plt.Figure:
    """Histogram of ACF cosine similarity across lake pairs.

    Args:
        summary_df: DataFrame with column acf_cos_sim.

    Returns:
        Matplotlib Figure.
    """
    values = summary_df["acf_cos_sim"].dropna()
    fig, ax = plt.subplots(figsize=(8, 5))
    if len(values) == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig
    ax.hist(values, bins=40, density=True, alpha=0.6, edgecolor="white", linewidth=0.4)
    mean_val = float(values.mean())
    ax.axvline(mean_val, color="red", linestyle="--", label=f"平均值 = {mean_val:.3f}")
    ax.set_xlabel("ACF余弦相似性")
    ax.set_ylabel("密度")
    ax.set_title("ACF余弦相似性分布")
    ax.legend()
    ax.set_xlim(-1.05, 1.05)
    fig.tight_layout()
    log.debug("plot_acf_cosine_distribution: n=%d", len(values))
    return fig


def plot_pearson_vs_acf(summary_df: pd.DataFrame) -> plt.Figure:
    """Scatter plot: Pearson r vs ACF cosine similarity.

    Args:
        summary_df: DataFrame with columns pearson_r and acf_cos_sim.

    Returns:
        Matplotlib Figure.
    """
    df = summary_df[["pearson_r", "acf_cos_sim"]].dropna()
    fig, ax = plt.subplots(figsize=(8, 6))
    if len(df) < 2:
        ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig
    ax.scatter(df["pearson_r"], df["acf_cos_sim"], alpha=0.4, s=10, rasterized=True)
    ax.axhline(0, color="gray", linestyle=":", linewidth=0.8)
    ax.axvline(0, color="gray", linestyle=":", linewidth=0.8)
    ax.set_xlabel("Pearson相关系数")
    ax.set_ylabel("ACF余弦相似性")
    ax.set_title("Pearson相关系数和ACF余弦相似性比较")
    ax.set_xlim(-1.05, 1.05)
    ax.set_ylim(-1.05, 1.05)
    ax.set_aspect("equal")
    corr = np.corrcoef(df["pearson_r"], df["acf_cos_sim"])[0, 1]
    ax.text(
        0.02, 0.98, f"n = {len(df):,}\ncorr = {corr:.3f}",
        transform=ax.transAxes, va="top", fontsize=9,
    )
    fig.tight_layout()
    log.debug("plot_pearson_vs_acf: n=%d", len(df))
    return fig
