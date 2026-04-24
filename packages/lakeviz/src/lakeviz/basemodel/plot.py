"""Baseline-model selection and fit diagnostics plots.

All functions accept only plain Python types and pandas DataFrames —
no lakeanalysis domain types are required.
"""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_candidate_scores(
    scores_df: pd.DataFrame,
    criterion: str,
    selected_basis_name: str,
) -> plt.Figure:
    """Plot candidate model scores and RMSE values.

    Parameters
    ----------
    scores_df: columns [basis_name, aic, bic, rmse]
    criterion: "aic" or "bic" — selects which score column to plot.
    selected_basis_name: Name of the selected model (highlighted in red).
    """
    names = scores_df["basis_name"].tolist()
    scores = scores_df[criterion].tolist()
    rmse = scores_df["rmse"].tolist()
    colors = ["tomato" if name == selected_basis_name else "steelblue" for name in names]

    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 4), constrained_layout=True)
    ax_left.bar(names, scores, color=colors)
    ax_left.set_title(f"候选模型{criterion.upper()}对比")
    ax_left.set_ylabel(criterion.upper())
    ax_left.tick_params(axis="x", rotation=20)
    ax_right.bar(names, rmse, color=colors)
    ax_right.set_title("候选模型RMSE对比")
    ax_right.set_ylabel("RMSE")
    ax_right.tick_params(axis="x", rotation=20)
    return fig


def plot_basis_fit(
    fit_frame: pd.DataFrame,
    selected_basis_name: str,
    criterion: str,
    relative_rmse: float | None,
) -> plt.Figure:
    """Plot observed values against fitted values with bias annotation.

    Parameters
    ----------
    fit_frame: columns [time, value, fitted, residual]
    selected_basis_name: Model name (title annotation).
    criterion: Selection criterion (title annotation).
    relative_rmse: Relative RMSE (title annotation).
    """
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(fit_frame["time"], fit_frame["value"], label="原始序列", color="steelblue", linewidth=1.2)
    ax.plot(fit_frame["time"], fit_frame["fitted"], label="拟合序列", color="tomato", linewidth=1.2)
    ax.set_xlabel("时间（年）")
    ax.set_ylabel("值")
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
    return fig


def plot_residuals(fit_frame: pd.DataFrame) -> plt.Figure:
    """Plot residual timeline and residual distribution.

    Parameters
    ----------
    fit_frame: columns [time, residual]
    """
    fig, (ax_left, ax_right) = plt.subplots(1, 2, figsize=(12, 4), constrained_layout=True)
    ax_left.plot(fit_frame["time"], fit_frame["residual"], color="purple", linewidth=1.0)
    ax_left.axhline(0.0, color="grey", linestyle="--", linewidth=1.0)
    ax_left.set_title("残差时序图")
    ax_left.set_xlabel("时间（年）")
    ax_left.set_ylabel("残差")

    ax_right.hist(fit_frame["residual"], bins=20, color="darkcyan", alpha=0.8)
    ax_right.set_title("残差分布图")
    ax_right.set_xlabel("残差")
    ax_right.set_ylabel("频数")
    return fig
