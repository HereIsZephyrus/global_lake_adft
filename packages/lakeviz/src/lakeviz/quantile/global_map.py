"""Global distribution maps for quantile-based identification results.

Uses SQL-side aggregation + pcolormesh for memory-efficient rendering.
"""

from __future__ import annotations

from pathlib import Path

from lakesource.quantile.reader import (
    fetch_extremes_grid_agg,
    fetch_extremes_by_type_grid_agg,
    fetch_transitions_grid_agg,
    fetch_transitions_by_type_grid_agg,
)

from ..config import GlobalGridConfig
from ..grid_map_factory import make_grid_map


plot_extremes_density_map = make_grid_map(
    fetch_extremes_grid_agg,
    "mean_per_lake",
    title="分位数识别极端事件密度 (每湖事件数)",
    cbar_label="每湖事件数",
    sub_dir="quantile/extremes",
    filename="density.png",
)

plot_transition_density_map = make_grid_map(
    fetch_transitions_grid_agg,
    "mean_per_lake",
    title="分位数识别旱涝突变密度 (每湖事件数)",
    cbar_label="每湖事件数",
    sub_dir="quantile/transitions",
    filename="density.png",
)


def plot_extremes_by_type_map(
    config: GlobalGridConfig,
    event_type: str,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    labels = {"high": "高值", "low": "低值", "dry": "干旱", "wet": "湿润"}
    label = labels.get(event_type, event_type)

    def _filter_by_type(agg):
        return agg[agg["event_type"] == event_type].reset_index(drop=True)

    fn = make_grid_map(
        fetch_extremes_by_type_grid_agg,
        "mean_per_lake",
        title=f"分位数识别{label}极端事件密度 (每湖事件数)",
        cbar_label="每湖事件数",
        sub_dir="quantile/extremes",
        filename=f"density_{event_type}.png",
        pre_filter_fn=_filter_by_type,
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)


def plot_transition_by_type_map(
    config: GlobalGridConfig,
    transition_type: str,
    *,
    refresh: bool = False,
    data_dir: Path | None = None,
    min_lakes: int = 1,
) -> Path:
    labels = {"low_to_high": "低转高", "high_to_low": "高转低", "dry_to_wet": "旱转涝", "wet_to_dry": "涝转旱"}
    label = labels.get(transition_type, transition_type)

    def _filter_by_type(agg):
        return agg[agg["transition_type"] == transition_type].reset_index(drop=True)

    fn = make_grid_map(
        fetch_transitions_by_type_grid_agg,
        "mean_per_lake",
        title=f"分位数识别{label}突变密度 (每湖事件数)",
        cbar_label="每湖事件数",
        sub_dir="quantile/transitions",
        filename=f"density_{transition_type}.png",
        pre_filter_fn=_filter_by_type,
    )
    return fn(config, refresh=refresh, data_dir=data_dir, min_lakes=min_lakes)