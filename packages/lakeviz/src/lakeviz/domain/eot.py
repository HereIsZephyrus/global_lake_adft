"""Domain-level draw functions — EOT diagnostics.

Each function operates on one or more ``plt.Axes`` passed by the caller.
Convenience ``plot_*`` wrappers (backward-compatible) are provided at the
bottom of this file.
"""

from __future__ import annotations

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.fill import draw_fill_between
from lakeviz.draw.reference import draw_axhline, draw_axvline, draw_diagonal
from lakeviz.draw.annotate import draw_annotate_point
from lakeviz.style.base import AxisStyle, apply_axis_style, AxKind, stamp_ax
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import (
    EOT_LINE,
    EOT_EXTREME_HIGH,
    EOT_EXTREME_LOW,
    EOT_THRESHOLD,
    EOT_LOCATION,
)


def draw_mrl(
    ax: plt.Axes,
    mrl_df: pd.DataFrame,
    *,
    line_style: LineStyle = LineStyle(marker="o", markersize=3),
    axis_style: AxisStyle = AxisStyle(xlabel="阈值", ylabel="平均超额", title="Mean residual life图"),
) -> None:
    draw_line(ax, mrl_df["threshold"], mrl_df["mean_excess"], style=line_style)
    apply_axis_style(ax, axis_style)


def draw_parameter_stability(
    ax_xi: plt.Axes,
    ax_sigma: plt.Axes,
    stability_df: pd.DataFrame,
    *,
    line_style: LineStyle = LineStyle(marker="o", markersize=3),
    xi_axis: AxisStyle = AxisStyle(xlabel="阈值", ylabel="xi 形状参数", title="形状稳定性"),
    sigma_axis: AxisStyle = AxisStyle(xlabel="阈值", ylabel="sigma* 修改后的尺度", title="修改后的尺度稳定性"),
) -> None:
    draw_line(ax_xi, stability_df["threshold"], stability_df["shape_xi"], style=line_style)
    apply_axis_style(ax_xi, xi_axis)
    draw_line(ax_sigma, stability_df["threshold"], stability_df["modified_scale"], style=line_style)
    apply_axis_style(ax_sigma, sigma_axis)


def draw_extremes_timeline(
    ax: plt.Axes,
    series_df: pd.DataFrame,
    extremes: pd.DataFrame,
    *,
    direction: str = "high",
    threshold: float | None = None,
    threshold_curve_df: pd.DataFrame | None = None,
    line_style: LineStyle = EOT_LINE,
    threshold_style: LineStyle = EOT_THRESHOLD,
    extreme_style: ScatterStyle = ScatterStyle(color="black", s=18, zorder=3, label="去丛化极值"),
    axis_style: AxisStyle = AxisStyle(xlabel="时间(年)", ylabel="湖泊水面积(m²)", title="在月序列上去丛化极值"),
) -> None:
    times = series_df["time"].to_numpy(dtype=float)
    draw_line(ax, times, series_df["original_value"], style=line_style)

    if threshold_curve_df is not None:
        u_curve = threshold_curve_df["threshold"].to_numpy(dtype=float)
        displayed_u = -u_curve if direction == "low" else u_curve
        draw_line(
            ax,
            threshold_curve_df["time"].to_numpy(dtype=float),
            displayed_u,
            style=threshold_style.replace(label="时间可变阈值 u(t)"),
        )
    elif threshold is not None:
        displayed_threshold = -threshold if direction == "low" else threshold
        draw_axhline(ax, displayed_threshold, style=ReferenceLineStyle(color="tomato", linestyle="--", label="阈值"))

    if not extremes.empty:
        draw_scatter(ax, extremes["time"], extremes["original_value"], style=extreme_style)

    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_pp(
    ax: plt.Axes,
    pp_df: pd.DataFrame,
    *,
    scatter_style: ScatterStyle = ScatterStyle(s=16, alpha=0.7),
    axis_style: AxisStyle = AxisStyle(xlabel="经验概率", ylabel="模型概率", title="概率图"),
) -> None:
    draw_scatter(ax, pp_df["empirical_probability"], pp_df["model_probability"], style=scatter_style)
    draw_diagonal(ax)
    apply_axis_style(ax, axis_style)


def draw_qq(
    ax: plt.Axes,
    qq_df: pd.DataFrame,
    *,
    scatter_style: ScatterStyle = ScatterStyle(s=16, alpha=0.7),
    axis_style: AxisStyle = AxisStyle(xlabel="理论指数分位数", ylabel="经验变换残差", title="分位数图"),
) -> None:
    draw_scatter(ax, qq_df["theoretical_quantile"], qq_df["empirical_quantile"], style=scatter_style)
    lower = min(float(qq_df["theoretical_quantile"].min()), float(qq_df["empirical_quantile"].min()))
    upper = max(float(qq_df["theoretical_quantile"].max()), float(qq_df["empirical_quantile"].max()))
    ax.plot([lower, upper], [lower, upper], linestyle="--", color="grey")
    apply_axis_style(ax, axis_style)


def draw_return_levels(
    ax: plt.Axes,
    return_levels_df: pd.DataFrame,
    *,
    line_style: LineStyle = LineStyle(marker="o", linewidth=1.2),
    axis_style: AxisStyle = AxisStyle(xlabel="重返期(年)", ylabel="重返水平", title="重返水平图"),
) -> None:
    draw_line(ax, return_levels_df["return_period_years"], return_levels_df["return_level"], style=line_style)
    valid_ci = return_levels_df[["ci_lower", "ci_upper"]].notna().all(axis=1)
    if bool(valid_ci.any()):
        draw_fill_between(
            ax,
            return_levels_df.loc[valid_ci, "return_period_years"],
            return_levels_df.loc[valid_ci, "ci_lower"],
            return_levels_df.loc[valid_ci, "ci_upper"],
        )
    ax.set_xscale("log")
    apply_axis_style(ax, axis_style)


def draw_location_model(
    ax: plt.Axes,
    mu_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    *,
    threshold: float | None = None,
    threshold_curve_df: pd.DataFrame | None = None,
    mu_style: LineStyle = EOT_LOCATION,
    extreme_style: ScatterStyle = ScatterStyle(color="black", s=14, alpha=0.7, label="去丛化超阈值"),
    threshold_style: LineStyle = EOT_THRESHOLD,
    axis_style: AxisStyle = AxisStyle(xlabel="时间(年)", ylabel="变换值", title="拟合位置模型"),
) -> None:
    draw_line(ax, mu_df["time"], mu_df["mu"], style=mu_style)
    draw_scatter(ax, extremes_df["time"], extremes_df["value"], style=extreme_style)

    if threshold_curve_df is not None:
        draw_line(
            ax,
            threshold_curve_df["time"].to_numpy(dtype=float),
            threshold_curve_df["threshold"].to_numpy(dtype=float),
            style=threshold_style.replace(label="时间可变阈值 u(t)"),
        )
    elif threshold is not None:
        draw_axhline(ax, threshold, style=ReferenceLineStyle(color="tomato", linestyle="--", label="阈值"))

    apply_axis_style(ax, axis_style)
    ax.legend()


def draw_eot_extremes(
    ax: plt.Axes,
    hylak_id: int,
    series_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    *,
    annotate_top_n_each_tail: int = 8,
    line_style: LineStyle = EOT_LINE,
    high_style: ScatterStyle = EOT_EXTREME_HIGH,
    low_style: ScatterStyle = EOT_EXTREME_LOW,
    axis_style: AxisStyle | None = None,
) -> None:
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date")

    draw_line(ax, line_df["date"], line_df["water_area"], style=line_style)

    if extremes_df is not None and not extremes_df.empty:
        evt = extremes_df.copy()
        evt = evt.dropna(subset=["year", "month", "water_area"])
        evt["year"] = evt["year"].astype(int)
        evt["month"] = evt["month"].astype(int)
        evt["date"] = pd.to_datetime(dict(year=evt["year"], month=evt["month"], day=1))
        evt["tail"] = evt["tail"].astype(str)
        evt["threshold_at_event"] = pd.to_numeric(evt["threshold_at_event"], errors="coerce")

        high = evt[evt["tail"] == "high"].copy()
        low = evt[evt["tail"] == "low"].copy()

        if not high.empty:
            draw_scatter(ax, high["date"], high["water_area"], style=high_style)
            high["severity"] = high["water_area"] - high["threshold_at_event"]
            top_high = high.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_high.iterrows():
                draw_annotate_point(ax, f"{int(row['month']):02d}", (row["date"], row["water_area"]), color=high_style.color)

        if not low.empty:
            draw_scatter(ax, low["date"], low["water_area"], style=low_style)
            low["severity"] = low["threshold_at_event"] - low["water_area"]
            top_low = low.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_low.iterrows():
                draw_annotate_point(ax, f"{int(row['month']):02d}", (row["date"], row["water_area"]), xytext=(4, -10), color=low_style.color)

    if axis_style is None:
        axis_style = AxisStyle(
            xlabel="时间", ylabel="湖泊水面积(m²)",
            title=f"hylak_id={hylak_id} 月尺度时序与 EOT 异常",
        )
    apply_axis_style(ax, axis_style.replace(grid_alpha=0.22, grid_linestyle=":"))
    ax.legend()


def draw_extremes_with_hawkes(
    ax: plt.Axes,
    hylak_id: int,
    series_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    hawkes_df: pd.DataFrame,
    *,
    annotate_top_n_each_tail: int = 8,
    line_style: LineStyle = LineStyle(
        color="steelblue", linewidth=1.5, marker="o", markersize=2.5,
        label="水域面积", zorder=2, alpha=0.85,
    ),
    high_style: ScatterStyle = EOT_EXTREME_HIGH,
    low_style: ScatterStyle = EOT_EXTREME_LOW,
    axis_style: AxisStyle | None = None,
) -> None:
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date")

    draw_line(ax, line_df["date"], line_df["water_area"], style=line_style)

    if hawkes_df is not None and not hawkes_df.empty:
        haw = hawkes_df.copy()
        haw["year"] = haw["year"].astype(int)
        haw["month"] = haw["month"].astype(int)
        haw["direction"] = haw["direction"].astype(str)
        haw["date"] = pd.to_datetime(dict(year=haw["year"], month=haw["month"], day=1))
        for direction, color in (("D_to_W", "#8B008B"), ("W_to_D", "#D2691E")):
            subset = haw[haw["direction"] == direction]
            for _, row in subset.iterrows():
                d = row["date"]
                next_month = d + pd.DateOffset(months=1)
                ax.axvspan(d, next_month, alpha=0.18, color=color, zorder=1)

    if extremes_df is not None and not extremes_df.empty:
        evt = extremes_df.copy()
        evt = evt.dropna(subset=["year", "month", "water_area"])
        evt["year"] = evt["year"].astype(int)
        evt["month"] = evt["month"].astype(int)
        evt["date"] = pd.to_datetime(dict(year=evt["year"], month=evt["month"], day=1))
        evt["tail"] = evt["tail"].astype(str)
        evt["threshold_at_event"] = pd.to_numeric(evt["threshold_at_event"], errors="coerce")

        high = evt[evt["tail"] == "high"].copy()
        low = evt[evt["tail"] == "low"].copy()

        if not high.empty:
            draw_scatter(ax, high["date"], high["water_area"], style=high_style)
            high["severity"] = high["water_area"] - high["threshold_at_event"]
            top_high = high.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_high.iterrows():
                draw_annotate_point(ax, f"{int(row['month']):02d}", (row["date"], row["water_area"]), color=high_style.color)

        if not low.empty:
            draw_scatter(ax, low["date"], low["water_area"], style=low_style)
            low["severity"] = low["threshold_at_event"] - low["water_area"]
            top_low = low.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_low.iterrows():
                draw_annotate_point(ax, f"{int(row['month']):02d}", (row["date"], row["water_area"]), xytext=(4, -10), color=low_style.color)

    extra_handles: list[mpatches.Patch] = []
    if hawkes_df is not None and not hawkes_df.empty:
        d2w = hawkes_df[hawkes_df["direction"].astype(str) == "D_to_W"]
        w2d = hawkes_df[hawkes_df["direction"].astype(str) == "W_to_D"]
        if not d2w.empty:
            extra_handles.append(mpatches.Patch(facecolor="#8B008B", alpha=0.4, label="Hawkes 旱→涝显著月"))
        if not w2d.empty:
            extra_handles.append(mpatches.Patch(facecolor="#D2691E", alpha=0.4, label="Hawkes 涝→旱显著月"))

    if axis_style is None:
        axis_style = AxisStyle(
            xlabel="时间 (Year-Month)", ylabel="水域面积 (km²)",
            title=f"湖泊 {hylak_id} 面积变化时序图（EOT极端 + Hawkes方向性转移）",
        )
    apply_axis_style(ax, axis_style.replace(grid_alpha=0.3, grid_linestyle="--"))
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles + extra_handles, labels + [h.get_label() for h in extra_handles], loc="best", fontsize=10, framealpha=0.9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    plt.xticks(rotation=45)


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_mrl(mrl_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_mrl(ax, mrl_df)
    fig.tight_layout()
    return fig


def plot_parameter_stability(stability_df: pd.DataFrame) -> plt.Figure:
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 5), sharex=True)
    draw_parameter_stability(ax_left, ax_right, stability_df)
    fig.tight_layout()
    return fig


def plot_extremes_timeline(
    series_df: pd.DataFrame,
    extremes: pd.DataFrame,
    *,
    direction: str = "high",
    threshold: float | None = None,
    threshold_curve_df: pd.DataFrame | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 4))
    draw_extremes_timeline(ax, series_df, extremes, direction=direction, threshold=threshold, threshold_curve_df=threshold_curve_df)
    fig.tight_layout()
    return fig


def plot_pp(pp_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(6, 6))
    draw_pp(ax, pp_df)
    fig.tight_layout()
    return fig


def plot_qq(qq_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(6, 6))
    draw_qq(ax, qq_df)
    fig.tight_layout()
    return fig


def plot_return_levels(return_levels_df: pd.DataFrame) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_return_levels(ax, return_levels_df)
    fig.tight_layout()
    return fig


def plot_location_model(
    mu_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    *,
    threshold: float | None = None,
    threshold_curve_df: pd.DataFrame | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 4))
    draw_location_model(ax, mu_df, extremes_df, threshold=threshold, threshold_curve_df=threshold_curve_df)
    fig.tight_layout()
    return fig


def plot_eot_extremes_from_db(
    hylak_id: int,
    series_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    *,
    annotate_top_n_each_tail: int = 8,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(13, 4.8))
    draw_eot_extremes(ax, hylak_id, series_df, extremes_df, annotate_top_n_each_tail=annotate_top_n_each_tail)
    fig.tight_layout()
    return fig


def plot_extremes_with_hawkes(
    hylak_id: int,
    series_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    hawkes_df: pd.DataFrame,
    *,
    annotate_top_n_each_tail: int = 8,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(14, 6))
    draw_extremes_with_hawkes(ax, hylak_id, series_df, extremes_df, hawkes_df, annotate_top_n_each_tail=annotate_top_n_each_tail)
    fig.tight_layout()
    return fig