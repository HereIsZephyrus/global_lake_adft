"""Matplotlib plots for human-impact assessment of artificial vs natural lakes."""

from __future__ import annotations

import logging

import pandas as pd
import matplotlib.pyplot as plt

log = logging.getLogger(__name__)

COLOR_ARTIFICIAL = "#E74C3C"
COLOR_NATURAL = "#27AE60"
COLOR_DELTA = "#3498DB"


def plot_volatility_comparison(impact_df: pd.DataFrame) -> plt.Figure:
    """Side-by-side box plots: artificial vs natural for CV, pct_change_std, range_ratio."""
    metrics = ["cv", "pct_change_std", "range_ratio"]
    labels = ["变异系数 (CV)", "月际变化率 std", "极差比"]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    for ax, m, label in zip(axes, metrics, labels):
        col_a = f"{m}_a"
        col_n = f"{m}_n"
        vals_a = impact_df[col_a].dropna()
        vals_n = impact_df[col_n].dropna()
        if vals_a.empty and vals_n.empty:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            continue
        bp = ax.boxplot(
            [vals_a, vals_n],
            labels=["人工湖", "自然湖"],
            patch_artist=True,
            widths=0.5,
        )
        bp["boxes"][0].set_facecolor(COLOR_ARTIFICIAL)
        bp["boxes"][0].set_alpha(0.5)
        bp["boxes"][1].set_facecolor(COLOR_NATURAL)
        bp["boxes"][1].set_alpha(0.5)
        ax.set_title(label)
        ax.grid(axis="y", alpha=0.2, linestyle=":")
    fig.suptitle("人工湖 vs 自然湖 波动性指标对比", fontsize=13)
    fig.tight_layout()
    return fig


def plot_delta_cv_distribution(impact_df: pd.DataFrame) -> plt.Figure:
    """Histogram of delta_cv (artificial - natural)."""
    values = impact_df["delta_cv"].dropna()
    fig, ax = plt.subplots(figsize=(8, 5))
    if len(values) == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig
    ax.hist(
        values, bins=40, density=True, alpha=0.6,
        color=COLOR_DELTA, edgecolor="white", linewidth=0.4,
    )
    mean_val = float(values.mean())
    ax.axvline(0, color="gray", linestyle=":", linewidth=0.8)
    ax.axvline(mean_val, color="red", linestyle="--", label=f"平均值 = {mean_val:.3f}")
    ax.set_xlabel("ΔCV (人工湖 − 自然湖)")
    ax.set_ylabel("密度")
    ax.set_title("变异系数差异分布")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_anomaly_ratio_comparison(impact_df: pd.DataFrame) -> plt.Figure:
    """Box plot comparing anomaly_ratio between artificial and natural lakes."""
    vals_a = impact_df["anomaly_ratio_a"].dropna()
    vals_n = impact_df["anomaly_ratio_n"].dropna()
    fig, ax = plt.subplots(figsize=(6, 5))
    if vals_a.empty and vals_n.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig
    bp = ax.boxplot(
        [vals_a, vals_n],
        labels=["人工湖", "自然湖"],
        patch_artist=True,
        widths=0.5,
    )
    bp["boxes"][0].set_facecolor(COLOR_ARTIFICIAL)
    bp["boxes"][0].set_alpha(0.5)
    bp["boxes"][1].set_facecolor(COLOR_NATURAL)
    bp["boxes"][1].set_alpha(0.5)
    ax.set_ylabel("异常月占比")
    ax.set_title("Z-score 异常月占比对比")
    ax.grid(axis="y", alpha=0.2, linestyle=":")
    fig.tight_layout()
    return fig


def plot_typical_pair_timeline(
    df_a: pd.DataFrame,
    df_n: pd.DataFrame,
    pair_info: dict,
) -> plt.Figure:
    """Overlay timeline of an artificial-natural lake pair with anomaly markers.

    Args:
        df_a: DataFrame with columns [year, month, water_area] for artificial lake.
        df_n: Same for natural lake.
        pair_info: Dict with hylak_id, nearest_id, and optional event lists.
    """
    fig, ax = plt.subplots(figsize=(12, 5))

    if not df_a.empty:
        ts_a = pd.to_datetime(
            {"year": df_a["year"], "month": df_a["month"], "day": 1}
        )
        ax.plot(
            ts_a, df_a["water_area"], color=COLOR_ARTIFICIAL,
            linewidth=1.0, alpha=0.8,
            label=f"人工湖 {pair_info.get('hylak_id', '?')}",
        )

    if not df_n.empty:
        ts_n = pd.to_datetime(
            {"year": df_n["year"], "month": df_n["month"], "day": 1}
        )
        ax.plot(
            ts_n, df_n["water_area"], color=COLOR_NATURAL,
            linewidth=1.0, alpha=0.8,
            label=f"自然湖 {pair_info.get('nearest_id', '?')}",
        )

    ax.set_xlabel("时间")
    ax.set_ylabel("水域面积 (m²)")
    ax.set_title(f"人工湖-自然湖时序对比 (topo_level={pair_info.get('topo_level', '?')})")
    ax.legend()
    ax.grid(alpha=0.2, linestyle=":")
    fig.tight_layout()
    return fig
