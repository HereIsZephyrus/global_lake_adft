"""Domain-level draw functions — Lake-pair similarity (Pearson and ACF cosine)."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from lakeviz.draw.histogram import draw_histogram
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.reference import draw_axhline, draw_axvline
from lakeviz.draw.annotate import draw_text_box
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import SIMILARITY_SCATTER

log = logging.getLogger(__name__)


def draw_pearson_distribution(
    ax: plt.Axes,
    summary_df: pd.DataFrame,
    *,
    hist_style: HistogramStyle = HistogramStyle(density=True, alpha=0.6, edgecolor="white", linewidth=0.4, bins=40),
    axis_style: AxisStyle = AxisStyle(xlabel="Pearson r", ylabel="密度", title="Pearson相关系数分布"),
) -> None:
    values = summary_df["pearson_r"].dropna()
    if len(values) == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        return
    draw_histogram(ax, values, style=hist_style)
    mean_val = float(values.mean())
    draw_axhline(ax, mean_val, style=ReferenceLineStyle(color="red", linestyle="--", label=f"平均值 = {mean_val:.3f}"))
    ax.set_xlim(-1.05, 1.05)
    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_acf_cosine_distribution(
    ax: plt.Axes,
    summary_df: pd.DataFrame,
    *,
    hist_style: HistogramStyle = HistogramStyle(density=True, alpha=0.6, edgecolor="white", linewidth=0.4, bins=40),
    axis_style: AxisStyle = AxisStyle(xlabel="ACF余弦相似性", ylabel="密度", title="ACF余弦相似性分布"),
) -> None:
    values = summary_df["acf_cos_sim"].dropna()
    if len(values) == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        return
    draw_histogram(ax, values, style=hist_style)
    mean_val = float(values.mean())
    draw_axhline(ax, mean_val, style=ReferenceLineStyle(color="red", linestyle="--", label=f"平均值 = {mean_val:.3f}"))
    ax.set_xlim(-1.05, 1.05)
    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_pearson_vs_acf(
    ax: plt.Axes,
    summary_df: pd.DataFrame,
    *,
    scatter_style: ScatterStyle = SIMILARITY_SCATTER,
    axis_style: AxisStyle = AxisStyle(xlabel="Pearson相关系数", ylabel="ACF余弦相似性", title="Pearson相关系数和ACF余弦相似性比较"),
) -> None:
    df = summary_df[["pearson_r", "acf_cos_sim"]].dropna()
    if len(df) < 2:
        ax.text(0.5, 0.5, "Insufficient data", ha="center", va="center", transform=ax.transAxes)
        return
    draw_scatter(ax, df["pearson_r"], df["acf_cos_sim"], style=scatter_style)
    draw_axhline(ax, 0, style=ReferenceLineStyle(color="gray", linestyle=":", linewidth=0.8))
    draw_axvline(ax, 0, style=ReferenceLineStyle(color="gray", linestyle=":", linewidth=0.8))
    ax.set_xlim(-1.05, 1.05)
    ax.set_ylim(-1.05, 1.05)
    ax.set_aspect("equal")
    corr = np.corrcoef(df["pearson_r"], df["acf_cos_sim"])[0, 1]
    draw_text_box(ax, f"n = {len(df):,}\ncorr = {corr:.3f}", x=0.02, y=0.98, ha="left", va="top")
    apply_axis_style(ax, axis_style)


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_pearson_distribution(summary_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_pearson_distribution(ax, summary_df)
    fig.tight_layout()
    return fig


def plot_acf_cosine_distribution(summary_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_acf_cosine_distribution(ax, summary_df)
    fig.tight_layout()
    return fig


def plot_pearson_vs_acf(summary_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 6))
    draw_pearson_vs_acf(ax, summary_df)
    fig.tight_layout()
    return fig