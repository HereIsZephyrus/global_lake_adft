"""Domain-level draw functions — Apportionment Entropy (AE) exploration."""

from __future__ import annotations

import logging

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.axes import Axes
from scipy.stats import pearsonr, spearmanr

from lakeviz.draw.line import draw_line
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.bar import draw_bar
from lakeviz.draw.histogram import draw_histogram
from lakeviz.draw.reference import draw_axhline
from lakeviz.draw.annotate import draw_text_box
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import ENTROPY_AE_LINE, ENTROPY_ANOMALY_POS, ENTROPY_ANOMALY_NEG

log = logging.getLogger(__name__)


def draw_ae_distribution(
    ax: plt.Axes,
    summary_df: pd.DataFrame,
    *,
    hist_style: HistogramStyle = HistogramStyle(alpha=0.6, label="直方图"),
    axis_style: AxisStyle = AxisStyle(xlabel="整体分配熵", ylabel="密度", title="湖泊整体分配熵分布"),
) -> None:
    values = summary_df["ae_overall"].dropna()
    draw_histogram(ax, values, style=hist_style)

    kde_x = np.linspace(values.min(), values.max(), 300)
    from scipy.stats import gaussian_kde
    kde = gaussian_kde(values)
    draw_line(ax, kde_x, kde(kde_x), style=LineStyle(linewidth=2, label="核密度估计"))

    draw_axhline(ax, float(values.mean()), style=ReferenceLineStyle(color="red", linestyle="--", label=f"均值 = {values.mean():.3f}"))
    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_amplitude_histogram(
    ax: plt.Axes,
    summary_df: pd.DataFrame,
    *,
    axis_style: AxisStyle = AxisStyle(xlabel="季节振幅变异系数 (CV)", ylabel="频数", title="STL 季节振幅变异系数 (CV) 分布"),
) -> None:
    values = summary_df["mean_seasonal_amplitude"].dropna()
    values = np.abs(values)
    if len(values) == 0:
        ax.text(0.5, 0.5, "无有效振幅数据", ha="center", va="center", transform=ax.transAxes)
        return

    low = float(values.min())
    high = float(values.max() * 1.01)
    bins = np.linspace(low, high, 50)
    draw_histogram(ax, values, style=HistogramStyle(edgecolor="white", linewidth=0.4, color="steelblue", alpha=0.8, bins=50))
    apply_axis_style(ax, axis_style)


def draw_annual_ae(
    ax_line: plt.Axes,
    ax_bar: plt.Axes,
    hylak_id: int,
    annual_df: pd.DataFrame,
    trend: dict | None = None,
) -> None:
    years = annual_df["year"].to_numpy()
    ae = annual_df["AE"].to_numpy()

    draw_line(ax_line, years, ae, style=ENTROPY_AE_LINE)

    if trend and trend.get("sens_slope") is not None:
        slope = trend["sens_slope"]
        intercept = np.median(ae - slope * years)
        trend_line = slope * years + intercept
        direction = trend["mk_trend"]
        p_val = trend["mk_p"]
        sig = "*" if trend.get("mk_significant") else ""
        draw_line(ax_line, years, trend_line, style=LineStyle(linestyle="--", linewidth=1.5, label=f"Sen 斜率 ({direction}{sig}, p={p_val:.3f})"))

    draw_axhline(ax_line, 1.0, style=ReferenceLineStyle(color="grey", linestyle=":", linewidth=0.8, label="最大 = 1"))
    apply_axis_style(ax_line, AxisStyle(ylabel="AE（归一化）", title=f"湖泊 {hylak_id} 年度分配熵"))
    ax_line.legend(fontsize=8)

    anomaly = annual_df["AE_anomaly"].to_numpy()
    colors = ["steelblue" if v >= 0 else "tomato" for v in anomaly]
    draw_bar(ax_bar, years.tolist(), anomaly, style=ENTROPY_ANOMALY_POS, colors=colors)
    draw_axhline(ax_bar, 0, style=ReferenceLineStyle(color="black", linewidth=0.8))
    apply_axis_style(ax_bar, AxisStyle(xlabel="年份", ylabel="AE 异常（比特）", title="年度 AE 异常"))


def draw_trend_summary(
    ax_slope: plt.Axes,
    ax_change: plt.Axes,
    summary_df: pd.DataFrame,
) -> None:
    slopes = summary_df["sens_slope"].dropna()
    if len(slopes) > 0:
        draw_histogram(ax_slope, slopes, style=HistogramStyle(edgecolor="white", linewidth=0.4, bins=40))
        draw_axhline(ax_slope, 0, style=ReferenceLineStyle(color="red", linestyle="--", linewidth=1))
        apply_axis_style(ax_slope, AxisStyle(xlabel="Sen 斜率 (AE/年)", ylabel="频数", title=f"Sen 斜率分布 (n={len(slopes)})"))
        ax_slope.set_xlim(-0.02, 0.02)

    changes = summary_df["change_per_decade_pct"].dropna()
    if len(changes) > 0:
        draw_histogram(ax_change, changes, style=HistogramStyle(edgecolor="white", linewidth=0.4, bins=40))
        draw_axhline(ax_change, 0, style=ReferenceLineStyle(color="red", linestyle="--", linewidth=1))
        ax_change.set_xlim(-50, 50)
        apply_axis_style(ax_change, AxisStyle(xlabel="每十年变化 (%)", ylabel="频数", title=f"每十年变化分布 (n={len(changes)})"))


def draw_amplitude_vs_entropy(
    ax: plt.Axes,
    summary_df: pd.DataFrame,
    *,
    scatter_style: ScatterStyle = ScatterStyle(alpha=0.3, s=8, color="steelblue", rasterized=True),
    axis_style: AxisStyle = AxisStyle(xlabel="季节振幅变异系数 (CV)", ylabel="1 − AE", title="季节振幅CV关于AE的OLS回归"),
) -> None:
    df = summary_df[["mean_seasonal_amplitude", "ae_overall"]].dropna()
    if len(df) < 3:
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        return

    x = np.abs(df["mean_seasonal_amplitude"].to_numpy(dtype=float))
    y = 1.0 - df["ae_overall"].to_numpy(dtype=float)

    r, p_r = pearsonr(x, y)
    rho, p_rho = spearmanr(x, y)
    coeffs = np.polyfit(x, y, 1)
    x_line = np.linspace(x.min(), x.max(), 300)
    y_line = np.polyval(coeffs, x_line)

    draw_scatter(ax, x, y, style=scatter_style)
    draw_line(ax, x_line, y_line, style=LineStyle(color="tomato", linewidth=1.5, label="OLS 回归"))

    annotation = (
        f"Pearson  r = {r:.3f}  (p={p_r:.2e})\n"
        f"Spearman ρ = {rho:.3f}  (p={p_rho:.2e})\n"
        f"n = {len(x):,}"
    )
    draw_text_box(ax, annotation, x=0.97, y=0.05)
    ax.legend(fontsize=9)
    apply_axis_style(ax, axis_style)


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_ae_distribution(summary_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_ae_distribution(ax, summary_df)
    fig.tight_layout()
    return fig


def plot_amplitude_histogram(summary_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_amplitude_histogram(ax, summary_df)
    fig.tight_layout()
    return fig


def plot_annual_ae(hylak_id, annual_df, trend=None) -> plt.Figure:
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    draw_annual_ae(axes[0], axes[1], hylak_id, annual_df, trend)
    fig.tight_layout()
    return fig


def plot_trend_summary(summary_df) -> plt.Figure:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    draw_trend_summary(ax1, ax2, summary_df)
    fig.suptitle("湖泊分配熵变化趋势")
    fig.tight_layout()
    return fig


def plot_amplitude_vs_entropy(summary_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(7, 6))
    draw_amplitude_vs_entropy(ax, summary_df)
    fig.tight_layout()
    return fig