"""Global distribution maps for EOT results using SQL-side aggregation + pcolormesh."""

from __future__ import annotations

from pathlib import Path

from lakesource.eot.reader import (
    fetch_eot_convergence_grid_agg,
    fetch_eot_converged_grid_agg,
)

from ..config import GlobalGridConfig
from ..grid_map_factory import make_grid_map


def plot_eot_convergence_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    q_tag = f"q{threshold_quantile:.2f}"
    fn = make_grid_map(
        fetch_eot_convergence_grid_agg,
        "convergence_rate",
        title=f"EOT 收敛率 — {tail} tail, {q_tag}",
        cmap="RdYlGn",
        log_scale=False,
        vmin=0,
        vmax=1,
        cbar_label="收敛率",
        sub_dir=f"eot/{tail}/q{threshold_quantile:.4f}",
        filename="convergence_rate.png",
        extra_fetch_kwargs={"tail": tail, "threshold_quantile": threshold_quantile},
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)


def plot_eot_xi_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    q_tag = f"q{threshold_quantile:.2f}"
    fn = make_grid_map(
        fetch_eot_converged_grid_agg,
        "median_xi",
        title=f"EOT 中位数 ξ — {tail} tail, {q_tag}",
        cmap="RdBu_r",
        log_scale=False,
        cbar_label="中位数 ξ",
        sub_dir=f"eot/{tail}/q{threshold_quantile:.4f}",
        filename="median_xi.png",
        extra_fetch_kwargs={"tail": tail, "threshold_quantile": threshold_quantile},
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)


def plot_eot_sigma_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    q_tag = f"q{threshold_quantile:.2f}"
    fn = make_grid_map(
        fetch_eot_converged_grid_agg,
        "median_sigma",
        title=f"EOT 中位数 σ — {tail} tail, {q_tag}",
        cbar_label="中位数 σ",
        sub_dir=f"eot/{tail}/q{threshold_quantile:.4f}",
        filename="median_sigma.png",
        extra_fetch_kwargs={"tail": tail, "threshold_quantile": threshold_quantile},
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)


def plot_eot_extremes_frequency_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    q_tag = f"q{threshold_quantile:.2f}"
    fn = make_grid_map(
        fetch_eot_converged_grid_agg,
        "mean_extremes_freq",
        title=f"EOT 极端事件频率 — {tail} tail, {q_tag}",
        cbar_label="极端事件频率",
        sub_dir=f"eot/{tail}/q{threshold_quantile:.4f}",
        filename="extremes_frequency.png",
        extra_fetch_kwargs={"tail": tail, "threshold_quantile": threshold_quantile},
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)


def plot_eot_threshold_map(
    config: GlobalGridConfig,
    tail: str,
    threshold_quantile: float,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 3,
) -> Path:
    q_tag = f"q{threshold_quantile:.2f}"
    fn = make_grid_map(
        fetch_eot_converged_grid_agg,
        "median_threshold",
        title=f"EOT 中位数阈值 — {tail} tail, {q_tag}",
        cbar_label="中位数阈值",
        sub_dir=f"eot/{tail}/q{threshold_quantile:.4f}",
        filename="median_threshold.png",
        extra_fetch_kwargs={"tail": tail, "threshold_quantile": threshold_quantile},
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)