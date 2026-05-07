"""Domain-level draw functions — artificial vs natural lake impact plots."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.draw.histogram import draw_histogram
from lakeviz.draw.reference import draw_axvline
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import IMPACT_ARTIFICIAL, IMPACT_NATURAL


_COLOR_ARTIFICIAL = "#E74C3C"
_COLOR_NATURAL = "#27AE60"
_COLOR_DELTA = "#3498DB"


def draw_volatility_comparison(
    axes: list[plt.Axes] | tuple[plt.Axes, ...],
    impact_df: pd.DataFrame,
) -> None:
    metrics = ["cv", "pct_change_std", "range_ratio"]
    labels = ["变异系数 (CV)", "月际变化率 std", "极差比"]
    for ax, metric, label in zip(axes, metrics, labels, strict=True):
        col_a = f"{metric}_a"
        col_n = f"{metric}_n"
        vals_a = impact_df[col_a].dropna()
        vals_n = impact_df[col_n].dropna()
        if vals_a.empty and vals_n.empty:
            ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
            apply_axis_style(ax, AxisStyle(title=label))
            continue
        bp = ax.boxplot(
            [vals_a, vals_n],
            labels=["人工湖", "自然湖"],
            patch_artist=True,
            widths=0.5,
        )
        bp["boxes"][0].set_facecolor(_COLOR_ARTIFICIAL)
        bp["boxes"][0].set_alpha(0.5)
        bp["boxes"][1].set_facecolor(_COLOR_NATURAL)
        bp["boxes"][1].set_alpha(0.5)
        apply_axis_style(ax, AxisStyle(title=label).replace(grid_alpha=0.2, grid_linestyle=":"))


def draw_delta_cv_distribution(
    ax: plt.Axes,
    impact_df: pd.DataFrame,
    *,
    hist_style: HistogramStyle = HistogramStyle(
        density=True,
        alpha=0.6,
        color=_COLOR_DELTA,
        edgecolor="white",
        linewidth=0.4,
        bins=40,
    ),
    axis_style: AxisStyle = AxisStyle(xlabel="ΔCV (人工湖 − 自然湖)", ylabel="密度", title="变异系数差异分布"),
) -> None:
    values = impact_df["delta_cv"].dropna()
    if len(values) == 0:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        return
    draw_histogram(ax, values, style=hist_style)
    mean_val = float(values.mean())
    draw_axvline(ax, 0, style=ReferenceLineStyle(color="gray", linestyle=":", linewidth=0.8))
    draw_axvline(ax, mean_val, style=ReferenceLineStyle(color="red", linestyle="--", label=f"平均值 = {mean_val:.3f}"))
    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_anomaly_ratio_comparison(ax: plt.Axes, impact_df: pd.DataFrame) -> None:
    vals_a = impact_df["anomaly_ratio_a"].dropna()
    vals_n = impact_df["anomaly_ratio_n"].dropna()
    if vals_a.empty and vals_n.empty:
        ax.text(0.5, 0.5, "No data", ha="center", va="center", transform=ax.transAxes)
        apply_axis_style(ax, AxisStyle(ylabel="异常月占比", title="Z-score 异常月占比对比"))
        return
    bp = ax.boxplot(
        [vals_a, vals_n],
        labels=["人工湖", "自然湖"],
        patch_artist=True,
        widths=0.5,
    )
    bp["boxes"][0].set_facecolor(_COLOR_ARTIFICIAL)
    bp["boxes"][0].set_alpha(0.5)
    bp["boxes"][1].set_facecolor(_COLOR_NATURAL)
    bp["boxes"][1].set_alpha(0.5)
    apply_axis_style(ax, AxisStyle(ylabel="异常月占比", title="Z-score 异常月占比对比").replace(grid_alpha=0.2, grid_linestyle=":"))


def draw_typical_pair_timeline(
    ax: plt.Axes,
    df_a: pd.DataFrame,
    df_n: pd.DataFrame,
    pair_info: dict,
    *,
    artificial_style: LineStyle = IMPACT_ARTIFICIAL,
    natural_style: LineStyle = IMPACT_NATURAL,
) -> None:
    if not df_a.empty:
        ts_a = pd.to_datetime({"year": df_a["year"], "month": df_a["month"], "day": 1})
        draw_line(
            ax,
            ts_a,
            df_a["water_area"],
            style=artificial_style.replace(label=f"人工湖 {pair_info.get('hylak_id', '?')}", alpha=0.8),
        )

    if not df_n.empty:
        ts_n = pd.to_datetime({"year": df_n["year"], "month": df_n["month"], "day": 1})
        draw_line(
            ax,
            ts_n,
            df_n["water_area"],
            style=natural_style.replace(label=f"自然湖 {pair_info.get('nearest_id', '?')}", alpha=0.8),
        )

    apply_axis_style(
        ax,
        AxisStyle(
            xlabel="时间",
            ylabel="水域面积 (m²)",
            title=f"人工湖-自然湖时序对比 (topo_level={pair_info.get('topo_level', '?')})",
        ).replace(grid_alpha=0.2, grid_linestyle=":"),
    )
    ax.legend()


def plot_volatility_comparison(impact_df: pd.DataFrame) -> plt.Figure:
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))
    draw_volatility_comparison(axes, impact_df)
    fig.suptitle("人工湖 vs 自然湖 波动性指标对比", fontsize=13)
    fig.tight_layout()
    return fig


def plot_delta_cv_distribution(impact_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_delta_cv_distribution(ax, impact_df)
    fig.tight_layout()
    return fig


def plot_anomaly_ratio_comparison(impact_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(6, 5))
    draw_anomaly_ratio_comparison(ax, impact_df)
    fig.tight_layout()
    return fig


def plot_typical_pair_timeline(df_a: pd.DataFrame, df_n: pd.DataFrame, pair_info: dict) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 5))
    draw_typical_pair_timeline(ax, df_a, df_n, pair_info)
    fig.tight_layout()
    return fig
