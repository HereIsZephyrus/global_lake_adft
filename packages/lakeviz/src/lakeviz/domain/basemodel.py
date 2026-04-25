"""Domain-level draw functions — Basemodel diagnostics."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.draw.bar import draw_bar
from lakeviz.draw.histogram import draw_histogram
from lakeviz.draw.reference import draw_axhline
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import BASEMODEL_ORIGINAL, BASEMODEL_FITTED, BASEMODEL_RESIDUAL


def draw_candidate_scores(
    ax_score: plt.Axes,
    ax_rmse: plt.Axes,
    scores_df: pd.DataFrame,
    criterion: str,
    selected_basis_name: str,
    *,
    score_axis: AxisStyle | None = None,
    rmse_axis: AxisStyle | None = None,
) -> None:
    names = scores_df["basis_name"].tolist()
    scores = scores_df[criterion].tolist()
    rmse = scores_df["rmse"].tolist()
    colors = ["tomato" if name == selected_basis_name else "steelblue" for name in names]

    draw_bar(ax_score, names, scores, style=BarStyle(), colors=colors)
    if score_axis is None:
        score_axis = AxisStyle(title=f"候选模型{criterion.upper()}对比", ylabel=criterion.upper(), x_rotation=20)
    apply_axis_style(ax_score, score_axis)

    draw_bar(ax_rmse, names, rmse, style=BarStyle(), colors=colors)
    if rmse_axis is None:
        rmse_axis = AxisStyle(title="候选模型RMSE对比", ylabel="RMSE", x_rotation=20)
    apply_axis_style(ax_rmse, rmse_axis)


def draw_basis_fit(
    ax: plt.Axes,
    fit_frame: pd.DataFrame,
    selected_basis_name: str,
    criterion: str,
    relative_rmse: float | None,
    *,
    original_style: LineStyle = BASEMODEL_ORIGINAL,
    fitted_style: LineStyle = BASEMODEL_FITTED,
) -> None:
    draw_line(ax, fit_frame["time"], fit_frame["value"], style=original_style)
    draw_line(ax, fit_frame["time"], fit_frame["fitted"], style=fitted_style)
    apply_axis_style(ax, AxisStyle(xlabel="时间（年）", ylabel="值"))
    ax.legend()

    bias_mean = float(fit_frame["residual"].mean())
    bias_std = float(fit_frame["residual"].std(ddof=1))
    p95_abs = float(np.percentile(np.abs(fit_frame["residual"]), 95))
    rr_text = "N/A" if relative_rmse is None else f"{relative_rmse:.4f}"
    ax.set_title(
        "最佳基准模型拟合\n"
        f"模型: {selected_basis_name} | 准则: {criterion.upper()} | 相对RMSE: {rr_text}\n"
        f"偏差均值: {bias_mean:.4f}, 偏差标准差: {bias_std:.4f}, |偏差|95分位: {p95_abs:.4f}"
    )


def draw_residuals(
    ax_ts: plt.Axes,
    ax_hist: plt.Axes,
    fit_frame: pd.DataFrame,
    *,
    ts_style: LineStyle = BASEMODEL_RESIDUAL,
    hist_style: HistogramStyle = HistogramStyle(color="darkcyan", alpha=0.8, bins=20),
) -> None:
    draw_line(ax_ts, fit_frame["time"], fit_frame["residual"], style=ts_style)
    draw_axhline(ax_ts, 0.0, style=ReferenceLineStyle(color="grey", linestyle="--", linewidth=1.0))
    apply_axis_style(ax_ts, AxisStyle(title="残差时序图", xlabel="时间（年）", ylabel="残差"))

    draw_histogram(ax_hist, fit_frame["residual"], style=hist_style)
    apply_axis_style(ax_hist, AxisStyle(title="残差分布图", xlabel="残差", ylabel="频数"))


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_candidate_scores(scores_df, criterion, selected_basis_name) -> plt.Figure:
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 4), constrained_layout=True)
    draw_candidate_scores(ax_left, ax_right, scores_df, criterion, selected_basis_name)
    return fig


def plot_basis_fit(fit_frame, selected_basis_name, criterion, relative_rmse) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 4))
    draw_basis_fit(ax, fit_frame, selected_basis_name, criterion, relative_rmse)
    return fig


def plot_residuals(fit_frame) -> plt.Figure:
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 4), constrained_layout=True)
    draw_residuals(ax_left, ax_right, fit_frame)
    return fig