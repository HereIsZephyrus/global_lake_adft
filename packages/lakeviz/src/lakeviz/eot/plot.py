"""EOT threshold diagnostics and NHPP model evaluation plots.

All functions accept only plain Python types and pandas DataFrames —
no lakeanalysis domain types are required.
"""

from __future__ import annotations

import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.plot_config import setup_chinese_font
from lakeviz.primitives import annotate_point

setup_chinese_font()


def plot_mrl(mrl_df: pd.DataFrame) -> plt.Figure:
    """Plot mean residual life diagnostics.

    Parameters
    ----------
    mrl_df: columns [threshold, mean_excess]
    """
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(mrl_df["threshold"], mrl_df["mean_excess"], marker="o", linewidth=1.2, markersize=3)
    ax.set_xlabel("阈值")
    ax.set_ylabel("平均超额")
    ax.set_title("Mean residual life图")
    fig.tight_layout()
    return fig


def plot_parameter_stability(stability_df: pd.DataFrame) -> plt.Figure:
    """Plot shape and modified scale stability across thresholds.

    Parameters
    ----------
    stability_df: columns [threshold, shape_xi, modified_scale]
    """
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 5), sharex=True)
    ax_left.plot(stability_df["threshold"], stability_df["shape_xi"], marker="o", linewidth=1.2, markersize=3)
    ax_left.set_xlabel("阈值")
    ax_left.set_ylabel("xi 形状参数")
    ax_left.set_title("形状稳定性")
    ax_right.plot(stability_df["threshold"], stability_df["modified_scale"], marker="o", linewidth=1.2, markersize=3)
    ax_right.set_xlabel("阈值")
    ax_right.set_ylabel("sigma* 修改后的尺度")
    ax_right.set_title("修改后的尺度稳定性")
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
    """Plot the monthly series with declustered exceedance representatives.

    Parameters
    ----------
    series_df: columns [time, original_value]
    extremes: columns [time, original_value]
    direction: "high" or "low" — controls threshold sign flip.
    threshold: scalar threshold (used when *threshold_curve_df* is None).
    threshold_curve_df: columns [time, threshold] — time-varying threshold u(t).
    """
    fig, ax = plt.subplots(figsize=(12, 4))
    times = series_df["time"].to_numpy(dtype=float)
    ax.plot(times, series_df["original_value"], linewidth=1.0, color="steelblue", label="月序列")

    if threshold_curve_df is not None:
        u_curve = threshold_curve_df["threshold"].to_numpy(dtype=float)
        displayed_u = -u_curve if direction == "low" else u_curve
        ax.plot(threshold_curve_df["time"].to_numpy(dtype=float), displayed_u, color="tomato", linestyle="--", linewidth=1.2, label="时间可变阈值 u(t)")
    elif threshold is not None:
        displayed_threshold = -threshold if direction == "low" else threshold
        ax.axhline(displayed_threshold, color="tomato", linestyle="--", linewidth=1.0, label="阈值")

    if not extremes.empty:
        ax.scatter(extremes["time"], extremes["original_value"], color="black", s=18, zorder=3, label="去丛化极值")
    ax.set_xlabel("时间(年)")
    ax.set_ylabel("湖泊水面积(m²)")
    ax.set_title("在月序列上去丛化极值")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_pp(pp_df: pd.DataFrame) -> plt.Figure:
    """Plot the residual probability plot.

    Parameters
    ----------
    pp_df: columns [empirical_probability, model_probability]
    """
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(pp_df["empirical_probability"], pp_df["model_probability"], s=16, alpha=0.7)
    ax.plot([0.0, 1.0], [0.0, 1.0], linestyle="--", color="grey")
    ax.set_xlabel("经验概率")
    ax.set_ylabel("模型概率")
    ax.set_title("概率图")
    fig.tight_layout()
    return fig


def plot_qq(qq_df: pd.DataFrame) -> plt.Figure:
    """Plot the residual quantile plot.

    Parameters
    ----------
    qq_df: columns [theoretical_quantile, empirical_quantile]
    """
    fig, ax = plt.subplots(figsize=(6, 6))
    ax.scatter(qq_df["theoretical_quantile"], qq_df["empirical_quantile"], s=16, alpha=0.7)
    lower = min(float(qq_df["theoretical_quantile"].min()), float(qq_df["empirical_quantile"].min()))
    upper = max(float(qq_df["theoretical_quantile"].max()), float(qq_df["empirical_quantile"].max()))
    ax.plot([lower, upper], [lower, upper], linestyle="--", color="grey")
    ax.set_xlabel("理论指数分位数")
    ax.set_ylabel("经验变换残差")
    ax.set_title("分位数图")
    fig.tight_layout()
    return fig


def plot_return_levels(return_levels_df: pd.DataFrame) -> plt.Figure:
    """Plot return level estimates and confidence intervals.

    Parameters
    ----------
    return_levels_df: columns [return_period_years, return_level, ci_lower, ci_upper]
    """
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(return_levels_df["return_period_years"], return_levels_df["return_level"], marker="o", linewidth=1.2)
    valid_ci = return_levels_df[["ci_lower", "ci_upper"]].notna().all(axis=1)
    if bool(valid_ci.any()):
        ax.fill_between(
            return_levels_df.loc[valid_ci, "return_period_years"],
            return_levels_df.loc[valid_ci, "ci_lower"],
            return_levels_df.loc[valid_ci, "ci_upper"],
            alpha=0.2,
        )
    ax.set_xscale("log")
    ax.set_xlabel("重返期(年)")
    ax.set_ylabel("重返水平")
    ax.set_title("重返水平图")
    fig.tight_layout()
    return fig


def plot_location_model(
    mu_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    *,
    threshold: float | None = None,
    threshold_curve_df: pd.DataFrame | None = None,
) -> plt.Figure:
    """Plot the fitted seasonal location function mu(t) together with the threshold.

    Parameters
    ----------
    mu_df: columns [time, mu]
    extremes_df: columns [time, value]
    threshold: scalar threshold (used when *threshold_curve_df* is None).
    threshold_curve_df: columns [time, threshold] — time-varying threshold u(t).
    """
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(mu_df["time"], mu_df["mu"], linewidth=1.5, color="purple", label="mu(t)")
    ax.scatter(extremes_df["time"], extremes_df["value"], color="black", s=14, alpha=0.7, label="去丛化超阈值")

    if threshold_curve_df is not None:
        ax.plot(threshold_curve_df["time"].to_numpy(dtype=float), threshold_curve_df["threshold"].to_numpy(dtype=float), linestyle="--", color="tomato", linewidth=1.2, label="时间可变阈值 u(t)")
    elif threshold is not None:
        ax.axhline(threshold, linestyle="--", color="tomato", label="阈值")
    ax.set_xlabel("时间(年)")
    ax.set_ylabel("变换值")
    ax.set_title("拟合位置模型")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_eot_extremes_from_db(
    hylak_id: int,
    series_df: pd.DataFrame,
    extremes_df: pd.DataFrame,
    *,
    annotate_top_n_each_tail: int = 8,
) -> plt.Figure:
    """Plot one-lake monthly series and annotate high/low EOT anomalies.

    Parameters
    ----------
    hylak_id: Lake identifier (title only).
    series_df: columns [year, month, water_area]
    extremes_df: columns [tail, year, month, water_area, threshold_at_event]
    annotate_top_n_each_tail: Number of strongest events to annotate per tail.
    """
    fig, ax = plt.subplots(figsize=(13, 4.8))
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date")
    ax.plot(line_df["date"], line_df["water_area"], color="steelblue", linewidth=1.1, label="月序列")

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
            ax.scatter(high["date"], high["water_area"], color="tomato", marker="^", s=42, zorder=4, label="高值异常(EOT)")
            high["severity"] = high["water_area"] - high["threshold_at_event"]
            top_high = high.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_high.iterrows():
                annotate_point(f"{int(row['month']):02d}", (row["date"], row["water_area"]), ax=ax, color="tomato")

        if not low.empty:
            ax.scatter(low["date"], low["water_area"], color="seagreen", marker="v", s=42, zorder=4, label="低值异常(EOT)")
            low["severity"] = low["threshold_at_event"] - low["water_area"]
            top_low = low.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_low.iterrows():
                annotate_point(f"{int(row['month']):02d}", (row["date"], row["water_area"]), ax=ax, xytext=(4, -10), color="seagreen")

    ax.set_xlabel("时间")
    ax.set_ylabel("湖泊水面积(m²)")
    ax.set_title(f"hylak_id={hylak_id} 月尺度时序与 EOT 异常")
    ax.grid(alpha=0.22, linestyle=":")
    ax.legend()
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
    """Plot monthly series with EOT extremes and Hawkes directional transitions.

    This is the primary web-facing chart combining three data layers:

    1. Monthly water-area time-series line
    2. EOT high/low extreme event scatter markers
    3. Hawkes significant directional-transition month bands (axvspan)

    Parameters
    ----------
    hylak_id: Lake identifier (title only).
    series_df: columns [year, month, water_area]
    extremes_df: columns [tail, year, month, water_area, threshold_at_event]. May be empty.
    hawkes_df: columns [year, month, direction] where direction is "D_to_W" or "W_to_D". May be empty.
    annotate_top_n_each_tail: Number of strongest EOT events to annotate per tail.
    """
    fig, ax = plt.subplots(figsize=(14, 6))

    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date")

    ax.plot(line_df["date"], line_df["water_area"], linewidth=1.5, color="steelblue", marker="o", markersize=2.5, label="水域面积", zorder=2, alpha=0.85)

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
            ax.scatter(high["date"], high["water_area"], color="#E74C3C", marker="^", s=55, zorder=4, label="EOT高值极端", edgecolors="#C0392B", linewidths=0.8)
            high["severity"] = high["water_area"] - high["threshold_at_event"]
            top_high = high.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_high.iterrows():
                annotate_point(f"{int(row['month']):02d}", (row["date"], row["water_area"]), ax=ax, color="#E74C3C")

        if not low.empty:
            ax.scatter(low["date"], low["water_area"], color="#27AE60", marker="v", s=55, zorder=4, label="EOT低值极端", edgecolors="#1E8449", linewidths=0.8)
            low["severity"] = low["threshold_at_event"] - low["water_area"]
            top_low = low.sort_values("severity", ascending=False).head(max(int(annotate_top_n_each_tail), 0))
            for _, row in top_low.iterrows():
                annotate_point(f"{int(row['month']):02d}", (row["date"], row["water_area"]), ax=ax, xytext=(4, -10), color="#27AE60")

    extra_handles: list[mpatches.Patch] = []
    if hawkes_df is not None and not hawkes_df.empty:
        d2w = hawkes_df[hawkes_df["direction"].astype(str) == "D_to_W"]
        w2d = hawkes_df[hawkes_df["direction"].astype(str) == "W_to_D"]
        if not d2w.empty:
            extra_handles.append(mpatches.Patch(facecolor="#8B008B", alpha=0.4, label="Hawkes 旱→涝显著月"))
        if not w2d.empty:
            extra_handles.append(mpatches.Patch(facecolor="#D2691E", alpha=0.4, label="Hawkes 涝→旱显著月"))

    ax.set_xlabel("时间 (Year-Month)", fontsize=12)
    ax.set_ylabel("水域面积 (km²)", fontsize=12)
    ax.set_title(f"湖泊 {hylak_id} 面积变化时序图（EOT极端 + Hawkes方向性转移）", fontsize=13, fontweight="bold")
    ax.grid(True, alpha=0.3, linestyle="--")
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles + extra_handles, labels + [h.get_label() for h in extra_handles], loc="best", fontsize=10, framealpha=0.9)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    plt.xticks(rotation=45)
    fig.tight_layout()
    return fig
