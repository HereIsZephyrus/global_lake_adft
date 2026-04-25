"""Global distribution maps for PWM extreme quantile results."""

from __future__ import annotations

from pathlib import Path

from lakesource.pwm_extreme.reader import (
    fetch_pwm_convergence_grid_agg,
    fetch_pwm_converged_grid_agg,
)

from ..config import GlobalGridConfig
from ..grid_map_factory import make_grid_map


plot_pwm_convergence_map = make_grid_map(
    fetch_pwm_convergence_grid_agg,
    "convergence_rate",
    title="PWM 极端分位数收敛率",
    cmap="RdYlGn",
    log_scale=False,
    vmin=0,
    vmax=1,
    cbar_label="收敛率",
    sub_dir="pwm_extreme",
    filename="convergence_rate.png",
)

plot_pwm_threshold_high_map = make_grid_map(
    fetch_pwm_converged_grid_agg,
    "median_threshold_high",
    title="PWM 极端分位数中位数高阈值",
    cbar_label="中位数高阈值",
    sub_dir="pwm_extreme",
    filename="median_threshold_high.png",
)

plot_pwm_threshold_low_map = make_grid_map(
    fetch_pwm_converged_grid_agg,
    "median_threshold_low",
    title="PWM 极端分位数中位数低阈值",
    cbar_label="中位数低阈值",
    sub_dir="pwm_extreme",
    filename="median_threshold_low.png",
)