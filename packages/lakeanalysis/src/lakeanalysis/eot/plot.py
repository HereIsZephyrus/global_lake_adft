"""Plot helpers for EOT threshold diagnostics and NHPP model evaluation.

Adapter layer: converts domain types to DataFrames, then delegates to
lakeviz primitives for rendering.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.primitives import (
    plot_line,
    plot_scatter,
    plot_scatter_with_diagonal,
    plot_bar,
    plot_fill_between,
    plot_axhline,
)
from lakeviz.plot_config import setup_chinese_font
from .diagnostics import ModelChecker, ReturnLevelEstimator
from .estimation import FitResult
from .preprocess import MonthlyTimeSeries

setup_chinese_font()


def plot_mrl(mrl_df: pd.DataFrame) -> plt.Figure:
    """Plot mean residual life diagnostics."""
    return plot_line(
        mrl_df["threshold"],
        mrl_df["mean_excess"],
        marker="o",
        markersize=3,
        linewidth=1.2,
        xlabel="阈值",
        ylabel="平均超额",
        title="Mean residual life图",
    )


def plot_parameter_stability(stability_df: pd.DataFrame) -> plt.Figure:
    """Plot shape and modified scale stability across thresholds."""
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 5), sharex=True)

    ax_left.plot(
        stability_df["threshold"],
        stability_df["shape_xi"],
        marker="o",
        linewidth=1.2,
        markersize=3,
    )
    ax_left.set_xlabel("阈值")
    ax_left.set_ylabel("xi 形状参数")
    ax_left.set_title("形状稳定性")

    ax_right.plot(
        stability_df["threshold"],
        stability_df["modified_scale"],
        marker="o",
        linewidth=1.2,
        markersize=3,
    )
    ax_right.set_xlabel("阈值")
    ax_right.set_ylabel("sigma* 修改后的尺度")
    ax_right.set_title("修改后的尺度稳定性")

    fig.tight_layout()
    return fig


def plot_extremes_timeline(
    series: MonthlyTimeSeries,
    extremes: pd.DataFrame,
    threshold: float,
    fit_result: "FitResult | None" = None,
) -> plt.Figure:
    """Plot the monthly series with declustered exceedance representatives."""
    displayed_series = (
        fit_result.full_series
        if fit_result is not None and fit_result.full_series is not None
        else series
    )
    times = displayed_series.data["time"].to_numpy(dtype=float)

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(
        times,
        displayed_series.data["original_value"],
        linewidth=1.0,
        color="steelblue",
        label="月序列",
    )

    if fit_result is not None and fit_result.threshold_params is not None:
        u_curve = fit_result.threshold_at(times)
        displayed_u = -u_curve if displayed_series.direction == "low" else u_curve
        ax.plot(
            times,
            displayed_u,
            color="tomato",
            linestyle="--",
            linewidth=1.2,
            label="时间可变阈值 u(t)",
        )
    else:
        displayed_threshold = -threshold if displayed_series.direction == "low" else threshold
        plot_axhline(displayed_threshold, ax=ax, label="阈值")

    if not extremes.empty:
        ax.scatter(
            extremes["time"],
            extremes["original_value"],
            color="black",
            s=18,
            zorder=3,
            label="去丛化极值",
        )
    ax.set_xlabel("时间(年)")
    ax.set_ylabel("湖泊水面积(m²)")
    ax.set_title("在月序列上去丛化极值")
    ax.legend()
    fig.tight_layout()
    return fig


def plot_pp(checker: ModelChecker) -> plt.Figure:
    """Plot the residual probability plot."""
    data = checker.probability_plot_data()
    return plot_scatter_with_diagonal(
        data["empirical_probability"],
        data["model_probability"],
        xlabel="经验概率",
        ylabel="模型概率",
        title="概率图",
    )


def plot_qq(checker: ModelChecker) -> plt.Figure:
    """Plot the residual quantile plot."""
    data = checker.quantile_plot_data()
    return plot_scatter_with_diagonal(
        data["theoretical_quantile"],
        data["empirical_quantile"],
        xlabel="理论指数分位数",
        ylabel="经验变换残差",
        title="分位数图",
    )


def plot_return_levels(
    return_levels: pd.DataFrame | ReturnLevelEstimator,
) -> plt.Figure:
    """Plot return level estimates and confidence intervals."""
    if isinstance(return_levels, ReturnLevelEstimator):
        data = return_levels.estimate()
    else:
        data = return_levels

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(
        data["return_period_years"],
        data["return_level"],
        marker="o",
        linewidth=1.2,
    )
    valid_ci = data[["ci_lower", "ci_upper"]].notna().all(axis=1)
    if bool(valid_ci.any()):
        plot_fill_between(
            data.loc[valid_ci, "return_period_years"],
            data.loc[valid_ci, "ci_lower"],
            data.loc[valid_ci, "ci_upper"],
            ax=ax,
        )
    ax.set_xscale("log")
    ax.set_xlabel("重返期(年)")
    ax.set_ylabel("重返水平")
    ax.set_title("重返水平图")
    fig.tight_layout()
    return fig


def plot_location_model(
    fit_result: FitResult,
    n_points: int = 400,
) -> plt.Figure:
    """Plot the fitted seasonal location function mu(t) together with the threshold."""
    reference_series = fit_result.full_series if fit_result.full_series is not None else fit_result.series
    grid = np.linspace(0.0, reference_series.duration_years, n_points)
    mu_values = fit_result.mu(grid)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(grid, mu_values, linewidth=1.5, color="purple", label="mu(t)")
    ax.scatter(
        fit_result.extremes["time"],
        fit_result.extremes["value"],
        color="black",
        s=14,
        alpha=0.7,
        label="去丛化超阈值",
    )
    if fit_result.threshold_params is not None:
        u_curve = fit_result.threshold_at(grid)
        ax.plot(
            grid,
            u_curve,
            linestyle="--",
            color="tomato",
            linewidth=1.2,
            label="时间可变阈值 u(t)",
        )
    else:
        plot_axhline(fit_result.threshold, ax=ax, label="阈值")
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
    annotate_top_n_each_tail: int = 8,
) -> plt.Figure:
    """Plot one-lake monthly series and annotate high/low EOT anomalies."""
    from lakeviz.primitives import annotate_point

    fig, ax = plt.subplots(figsize=(13, 4.8))

    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(
        dict(year=line_df["year"], month=line_df["month"], day=1)
    )
    line_df = line_df.sort_values("date")
    ax.plot(
        line_df["date"],
        line_df["water_area"],
        color="steelblue",
        linewidth=1.1,
        label="月序列",
    )

    if extremes_df is not None and not extremes_df.empty:
        evt = extremes_df.copy()
        evt = evt.dropna(subset=["year", "month", "water_area"])
        evt["year"] = evt["year"].astype(int)
        evt["month"] = evt["month"].astype(int)
        evt["date"] = pd.to_datetime(
            dict(year=evt["year"], month=evt["month"], day=1)
        )
        evt["tail"] = evt["tail"].astype(str)
        evt["threshold_at_event"] = pd.to_numeric(
            evt["threshold_at_event"], errors="coerce"
        )

        high = evt[evt["tail"] == "high"].copy()
        low = evt[evt["tail"] == "low"].copy()

        if not high.empty:
            ax.scatter(
                high["date"],
                high["water_area"],
                color="tomato",
                marker="^",
                s=42,
                zorder=4,
                label="高值异常(EOT)",
            )
            high["severity"] = high["water_area"] - high["threshold_at_event"]
            top_high = high.sort_values("severity", ascending=False).head(
                max(int(annotate_top_n_each_tail), 0)
            )
            for _, row in top_high.iterrows():
                annotate_point(
                    f"{int(row['month']):02d}",
                    (row["date"], row["water_area"]),
                    ax=ax,
                    color="tomato",
                )

        if not low.empty:
            ax.scatter(
                low["date"],
                low["water_area"],
                color="seagreen",
                marker="v",
                s=42,
                zorder=4,
                label="低值异常(EOT)",
            )
            low["severity"] = low["threshold_at_event"] - low["water_area"]
            top_low = low.sort_values("severity", ascending=False).head(
                max(int(annotate_top_n_each_tail), 0)
            )
            for _, row in top_low.iterrows():
                annotate_point(
                    f"{int(row['month']):02d}",
                    (row["date"], row["water_area"]),
                    ax=ax,
                    xytext=(4, -10),
                    color="seagreen",
                )

    ax.set_xlabel("时间")
    ax.set_ylabel("湖泊水面积(m²)")
    ax.set_title(f"hylak_id={hylak_id} 月尺度时序与 EOT 异常")
    ax.grid(alpha=0.22, linestyle=":")
    ax.legend()
    fig.tight_layout()
    return fig
