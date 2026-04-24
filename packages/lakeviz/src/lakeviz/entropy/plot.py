"""Matplotlib visualisation helpers for Apportionment Entropy (AE) exploration."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.axes import Axes
from scipy.stats import pearsonr, spearmanr

log = logging.getLogger(__name__)

AMPLITUDE_OUTLIER_IQR_MULT = 1.5


def remove_amplitude_outliers(summary_df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where |mean_seasonal_amplitude| is an IQR-based outlier."""
    col = "mean_seasonal_amplitude"
    if col not in summary_df.columns:
        return summary_df
    amp = summary_df[col].dropna()
    if len(amp) < 4:
        return summary_df
    a = np.abs(amp.to_numpy(dtype=float))
    q1, q3 = np.percentile(a, [25, 75])
    iqr = q3 - q1
    if iqr <= 0:
        return summary_df
    low = q1 - AMPLITUDE_OUTLIER_IQR_MULT * iqr
    high = q3 + AMPLITUDE_OUTLIER_IQR_MULT * iqr
    mask = (summary_df[col].notna()) & (
        (summary_df[col].abs() < low) | (summary_df[col].abs() > high)
    )
    n_removed = mask.sum()
    if n_removed > 0:
        log.debug("remove_amplitude_outliers: removed %d rows (IQR bounds [%.4g, %.4g])", n_removed, low, high)
    return summary_df.loc[~mask].copy()


def plot_ae_distribution(summary_df: pd.DataFrame) -> plt.Figure:
    """Histogram + KDE of overall AE values across all lakes."""
    values = summary_df["ae_overall"].dropna()

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.hist(values, bins=40, density=True, alpha=0.6, label="直方图")

    kde_x = np.linspace(values.min(), values.max(), 300)
    from scipy.stats import gaussian_kde

    kde = gaussian_kde(values)
    ax.plot(kde_x, kde(kde_x), linewidth=2, label="核密度估计")

    ax.axvline(float(values.mean()), color="red", linestyle="--", label=f"均值 = {values.mean():.3f}")
    ax.set_xlabel("整体分配熵")
    ax.set_ylabel("密度")
    ax.set_title("湖泊整体分配熵分布")
    ax.legend()
    fig.tight_layout()
    log.debug("plot_ae_distribution: n=%d values", len(values))
    return fig


def plot_amplitude_histogram(summary_df: pd.DataFrame) -> plt.Figure:
    """Histogram of STL seasonal amplitude CV."""
    values = summary_df["mean_seasonal_amplitude"].dropna()
    values = np.abs(values)
    if len(values) == 0:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, "无有效振幅数据", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig

    fig, ax = plt.subplots(figsize=(8, 5))
    low = float(values.min())
    high = float(values.max() * 1.01)
    bins = np.linspace(low, high, 50)
    ax.hist(values, bins=bins, edgecolor="white", linewidth=0.4, color="steelblue", alpha=0.8)
    ax.set_xlabel("季节振幅变异系数 (CV)")
    ax.set_ylabel("频数")
    ax.set_title("STL 季节振幅变异系数 (CV) 分布")
    fig.tight_layout()
    log.debug("plot_amplitude_histogram: n=%d values", len(values))
    return fig


def plot_annual_ae(
    hylak_id: int,
    annual_df: pd.DataFrame,
    trend: dict | None = None,
) -> plt.Figure:
    """Line chart of annual AE for a single lake, with optional Sen's trend line."""
    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

    _plot_ae_line(axes[0], annual_df, trend, title=f"湖泊 {hylak_id} 年度分配熵")
    _plot_anomaly_bar(axes[1], annual_df)

    fig.tight_layout()
    return fig


def _plot_ae_line(ax: Axes, annual_df: pd.DataFrame, trend: dict | None, title: str) -> None:
    years = annual_df["year"].to_numpy()
    ae = annual_df["AE"].to_numpy()

    ax.plot(years, ae, marker="o", markersize=3, linewidth=1, label="年度 AE")

    if trend and trend.get("sens_slope") is not None:
        slope = trend["sens_slope"]
        intercept = np.median(ae - slope * years)
        trend_line = slope * years + intercept
        direction = trend["mk_trend"]
        p_val = trend["mk_p"]
        sig = "*" if trend.get("mk_significant") else ""
        ax.plot(
            years,
            trend_line,
            linestyle="--",
            linewidth=1.5,
            label=f"Sen 斜率 ({direction}{sig}, p={p_val:.3f})",
        )

    ax.axhline(1.0, color="grey", linestyle=":", linewidth=0.8, label="最大 = 1")
    ax.set_ylabel("AE（归一化）")
    ax.set_title(title)
    ax.legend(fontsize=8)


def _plot_anomaly_bar(ax: Axes, annual_df: pd.DataFrame) -> None:
    years = annual_df["year"].to_numpy()
    anomaly = annual_df["AE_anomaly"].to_numpy()
    colors = ["steelblue" if v >= 0 else "tomato" for v in anomaly]
    ax.bar(years, anomaly, color=colors, width=0.8)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xlabel("年份")
    ax.set_ylabel("AE 异常（比特）")
    ax.set_title("年度 AE 异常")


def plot_trend_summary(summary_df: pd.DataFrame) -> plt.Figure:
    """Histograms of Sen's slope and change_per_decade_pct across all lakes."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

    slopes = summary_df["sens_slope"].dropna()
    if len(slopes) > 0:
        ax1.hist(slopes, bins=40, edgecolor="white", linewidth=0.4)
        ax1.axvline(0, color="red", linestyle="--", linewidth=1)
        ax1.set_xlabel("Sen 斜率 (AE/年)")
        ax1.set_ylabel("频数")
        ax1.set_title(f"Sen 斜率分布 (n={len(slopes)})")
        ax1.set_xlim(-0.02, 0.02)

    changes = summary_df["change_per_decade_pct"].dropna()
    if len(changes) > 0:
        ax2.hist(changes, bins=40, edgecolor="white", linewidth=0.4)
        ax2.axvline(0, color="red", linestyle="--", linewidth=1)
        ax2.set_xlim(-50, 50)
        ax2.set_xlabel("每十年变化 (%)")
        ax2.set_ylabel("频数")
        ax2.set_title(f"每十年变化分布 (n={len(changes)})")

    fig.suptitle("湖泊分配熵变化趋势")
    fig.tight_layout()
    log.debug("plot_trend_summary: slopes n=%d, changes n=%d", len(slopes), len(changes))
    return fig


def plot_amplitude_vs_entropy(summary_df: pd.DataFrame) -> plt.Figure:
    """Scatter: STL amplitude vs 1-AE with OLS regression."""
    df = summary_df[["mean_seasonal_amplitude", "ae_overall"]].dropna()
    if len(df) < 3:
        log.warning("plot_amplitude_vs_entropy: fewer than 3 valid rows, skipping")
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        return fig

    x = np.abs(df["mean_seasonal_amplitude"].to_numpy(dtype=float))
    y = 1.0 - df["ae_overall"].to_numpy(dtype=float)

    fig, ax_right = plt.subplots(figsize=(7, 6))

    if len(x) < 2:
        ax_right.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax_right.transAxes)
    else:
        r, p_r = pearsonr(x, y)
        rho, p_rho = spearmanr(x, y)
        coeffs = np.polyfit(x, y, 1)
        x_line = np.linspace(x.min(), x.max(), 300)
        y_line = np.polyval(coeffs, x_line)
        ax_right.scatter(x, y, alpha=0.3, s=8, color="steelblue", rasterized=True)
        ax_right.plot(x_line, y_line, color="tomato", linewidth=1.5, label="OLS 回归")
        annotation = (
            f"Pearson  r = {r:.3f}  (p={p_r:.2e})\n"
            f"Spearman ρ = {rho:.3f}  (p={p_rho:.2e})\n"
            f"n = {len(x):,}"
        )
        ax_right.text(0.97, 0.05, annotation, transform=ax_right.transAxes, ha="right", va="bottom", fontsize=9, bbox=dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.8))
        ax_right.legend(fontsize=9)
    ax_right.set_xlabel("季节振幅变异系数 (CV)")
    ax_right.set_ylabel("1 − AE")
    ax_right.set_title("季节振幅CV关于AE的OLS回归")

    fig.tight_layout()
    log.debug("plot_amplitude_vs_entropy: n=%d", len(df))
    return fig