"""Global distribution maps for quantile-based identification results.

Uses LakeProvider aggregation + pcolormesh for memory-efficient rendering.
"""

from __future__ import annotations

from pathlib import Path

from ..config import GlobalGridConfig
from ..grid_map_factory import make_density_map, make_grid_map


def _fetch_extremes_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_extremes_grid_agg(resolution, refresh=refresh)


def _fetch_extremes_by_type_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_extremes_by_type_grid_agg(resolution, refresh=refresh)


def _fetch_transitions_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_transitions_grid_agg(resolution, refresh=refresh)


def _fetch_transitions_by_type_grid_agg(provider, resolution, *, refresh=False):
    return provider.fetch_transitions_by_type_grid_agg(resolution, refresh=refresh)


plot_extremes_density_map = make_grid_map(
    _fetch_extremes_grid_agg,
    "mean_per_lake",
    title="分位数识别极端事件密度 (每湖事件数)",
    cbar_label="每湖事件数",
    sub_dir="quantile/extremes",
    filename="density.png",
)

plot_transition_density_map = make_grid_map(
    _fetch_transitions_grid_agg,
    "mean_per_lake",
    title="分位数识别旱涝急转转换数量全球分布 (每湖事件数)",
    cbar_label="每湖事件数",
    sub_dir="quantile/transitions",
    filename="count_grid.png",
)


def plot_extremes_by_type_map(
    config: GlobalGridConfig,
    event_type: str,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
) -> Path:
    canonical_type = {"wet": "high", "dry": "low"}.get(event_type, event_type)
    labels = {
        "high": "超高阈值（湿事件）",
        "low": "超低阈值（干事件）",
        "dry": "超低阈值（干事件）",
        "wet": "超高阈值（湿事件）",
    }
    cmap = "sequential_cool" if canonical_type == "high" else "sequential_warm"
    filename = "wet_grid.png" if canonical_type == "high" else "dry_grid.png"
    label = labels.get(event_type, event_type)

    def _filter_by_type(agg):
        return agg[agg["event_type"] == canonical_type].reset_index(drop=True)

    fn = make_grid_map(
        _fetch_extremes_by_type_grid_agg,
        "mean_per_lake",
        title=f"分位数识别{label}全球分布 (每湖事件数)",
        cmap=cmap,
        cbar_label="每湖事件数",
        sub_dir="quantile/extremes",
        filename=filename,
        pre_filter_fn=_filter_by_type,
    )
    return fn(config, refresh=refresh, min_lakes=min_lakes)


def plot_transition_by_type_map(
    config: GlobalGridConfig,
    transition_type: str,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
) -> Path:
    canonical_type = {
        "dry_to_wet": "low_to_high",
        "wet_to_dry": "high_to_low",
    }.get(transition_type, transition_type)
    labels = {
        "low_to_high": "旱转涝",
        "high_to_low": "涝转旱",
        "dry_to_wet": "旱转涝",
        "wet_to_dry": "涝转旱",
    }
    cmap = "sequential_cool" if transition_type == "dry_to_wet" else "sequential_warm"
    label = labels.get(transition_type, transition_type)

    def _filter_by_type(agg):
        return agg[agg["transition_type"] == canonical_type].reset_index(drop=True)

    fn = make_grid_map(
        _fetch_transitions_by_type_grid_agg,
        "mean_per_lake",
        title=f"分位数识别{label}事件全球分布 (每湖事件数)",
        cmap=cmap,
        cbar_label="每湖事件数",
        sub_dir="quantile/transitions",
        filename=f"{canonical_type}_grid.png",
        pre_filter_fn=_filter_by_type,
    )
    return fn(config, refresh=refresh, min_lakes=min_lakes)


plot_extremes_event_density_map = make_density_map(
    _fetch_extremes_grid_agg,
    "event_count",
    title="分位数识别极端事件密度 (事件总数)",
    cbar_label="事件数",
    sub_dir="quantile/extremes",
    filename="event_density.png",
)

plot_transition_event_density_map = make_density_map(
    _fetch_transitions_grid_agg,
    "event_count",
    title="分位数识别旱涝急转转换数量平滑密度分布 (事件总数)",
    cbar_label="事件数",
    sub_dir="quantile/transitions",
    filename="count_kde.png",
)


def plot_quantile_global_maps(
    config: GlobalGridConfig,
    *,
    refresh: bool = False,
    min_lakes: int = 1,
) -> list[Path]:
    """Generate the standard batch of quantile global maps."""
    outputs = [
        plot_extremes_by_type_map(config, "wet", refresh=refresh, min_lakes=min_lakes),
        plot_extremes_by_type_map(config, "dry", refresh=refresh, min_lakes=min_lakes),
        plot_transition_density_map(config, refresh=refresh, min_lakes=min_lakes),
        plot_transition_event_density_map(config, refresh=refresh, min_lakes=min_lakes),
        plot_transition_by_type_map(config, "dry_to_wet", refresh=refresh, min_lakes=min_lakes),
        plot_transition_by_type_map(config, "wet_to_dry", refresh=refresh, min_lakes=min_lakes),
    ]

    return [path for path in outputs if path != Path()]
