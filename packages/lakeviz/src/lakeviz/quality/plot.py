"""Area comparison visualization: scatter and histogram plots."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from lakeviz.plot_config import setup_chinese_font

log = logging.getLogger(__name__)

_AGREEMENT_COLORS = {
    "excellent": "#2ca02c",
    "good": "#98df8a",
    "moderate": "#ffbb78",
    "poor": "#ff7f0e",
    "extreme": "#d62728",
}

_DEFAULT_CONFIG = {"excellent": 0.1, "good": 2.0, "moderate": 5.0, "poor": 10.0}


@dataclass
class _ScatterData:
    """Validated data for scatter plot."""
    df: pd.DataFrame
    atlas_col: str
    rs_col: str
    agreement_col: str
    config: dict[str, float]


@dataclass
class _HistData:
    """Validated data for histogram."""
    df: pd.DataFrame
    log2_ratio_col: str
    agreement_col: str
    config: dict[str, float]


def _agreement_label(level: str, config: dict[str, float]) -> str:
    """Generate Chinese label with actual ratio range for agreement level."""
    t = config.get("excellent", 0.1)
    g = config.get("good", 2.0)
    m = config.get("moderate", 5.0)
    p = config.get("poor", 10.0)

    labels = {
        "excellent": f"±{int(t * 100)}% ({1 - t:.1f}~{1 + t:.1f})",
        "good": f"{1 / g:.1f}~{g:.0f}倍",
        "moderate": f"{1 / m:.1f}~{m:.0f}倍",
        "poor": f"{1 / p:.1f}~{p:.0f}倍",
        "extreme": f"<{1 / p:.1f} 或 >{p:.0f}倍",
    }
    return labels.get(level, level)


def _prepare_scatter(
    df: pd.DataFrame,
    atlas_col: str,
    rs_col: str,
    agreement_col: str,
    config: dict[str, float] | None,
) -> _ScatterData | None:
    """Validate and prepare data for scatter plot."""
    cfg = config if config is not None else _DEFAULT_CONFIG
    valid = df[[atlas_col, rs_col, agreement_col]].dropna()
    valid = valid[(valid[atlas_col] > 0) & (valid[rs_col] > 0)]
    if len(valid) < 2:
        return None
    return _ScatterData(valid, atlas_col, rs_col, agreement_col, cfg)


def _draw_scatter_points(ax: plt.Axes, data: _ScatterData) -> None:
    """Draw scatter points colored by agreement level."""
    for level, color in _AGREEMENT_COLORS.items():
        mask = data.df[data.agreement_col] == level
        subset = data.df[mask]
        if len(subset) == 0:
            continue
        label = f"{_agreement_label(level, data.config)} ({len(subset):,})"
        ax.scatter(
            subset[data.atlas_col],
            subset[data.rs_col],
            c=color, s=8, alpha=0.5,
            label=label, rasterized=True,
        )


def _draw_reference_curves(ax: plt.Axes, lo: float, hi: float) -> None:
    """Draw 1:1 and 2x reference lines."""
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=1.0, label="1:1线", zorder=5)
    ax.plot([lo, hi], [lo * 2, hi * 2], ":", color="grey", linewidth=0.8, label="2倍线", zorder=5)
    ax.plot([lo, hi], [lo / 2, hi / 2], ":", color="grey", linewidth=0.8, zorder=5)


def plot_area_scatter(
    df: pd.DataFrame,
    *,
    atlas_col: str = "atlas_area",
    rs_col: str = "rs_area_median",
    agreement_col: str = "agreement_median",
    title: str = "遥感面积与HydroATLAS面积对比",
    config: dict[str, float] | None = None,
) -> plt.Figure:
    """Scatter plot of rs_area vs atlas_area on log-log scale with 1:1 and ±2x lines."""
    setup_chinese_font()

    data = _prepare_scatter(df, atlas_col, rs_col, agreement_col, config)
    if data is None:
        fig, ax = plt.subplots(figsize=(8, 6))
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig

    fig, ax = plt.subplots(figsize=(8, 6))
    _draw_scatter_points(ax, data)

    lo = max(data.df[data.atlas_col].min(), data.df[data.rs_col].min()) * 0.5
    hi = max(data.df[data.atlas_col].max(), data.df[data.rs_col].max()) * 2
    _draw_reference_curves(ax, lo, hi)

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("HydroATLAS面积 (km²)")
    ax.set_ylabel("遥感面积 (km²)")
    ax.set_title(title)
    ax.legend(fontsize=8, loc="upper left")
    ax.set_xlim(lo, hi)
    ax.set_ylim(lo, hi)
    ax.set_aspect("equal")
    ax.grid(alpha=0.2, which="both")

    fig.tight_layout()
    log.debug("plot_area_scatter: n=%d", len(data.df))
    return fig


def _draw_reference_lines(ax: plt.Axes) -> None:
    """Draw ratio reference lines on histogram axis."""
    ax.axvline(0, color="black", linestyle="--", linewidth=1.0, label="比值=1")
    ax.axvline(1, color="grey", linestyle=":", linewidth=0.8, label="比值=2")
    ax.axvline(-1, color="grey", linestyle=":", linewidth=0.8, label="比值=0.5")


@dataclass
class _HistPlotParams:
    """Parameters for histogram plotting."""
    valid: pd.DataFrame
    log2_ratio_col: str
    agreement_col: str
    bins: np.ndarray
    config: dict[str, float]


def _draw_level_histograms(ax: plt.Axes, params: _HistPlotParams) -> None:
    """Draw histogram bars for each agreement level."""
    for level, color in _AGREEMENT_COLORS.items():
        mask = params.valid[params.agreement_col] == level
        subset = params.valid[mask][params.log2_ratio_col].values
        if len(subset) == 0:
            continue
        label = f"{_agreement_label(level, params.config)} ({len(subset):,})"
        ax.hist(
            subset, bins=params.bins, alpha=0.6, color=color,
            edgecolor="white", linewidth=0.3, label=label,
        )


def plot_ratio_histogram(
    df: pd.DataFrame,
    *,
    log2_ratio_col: str = "log2_ratio_median",
    agreement_col: str = "agreement_median",
    title: str = "log₂(遥感面积/HydroATLAS面积)分布",
    config: dict[str, float] | None = None,
) -> plt.Figure:
    """Histogram of log2(ratio) colored by agreement level."""
    setup_chinese_font()

    cfg = config if config is not None else _DEFAULT_CONFIG
    valid = df[[log2_ratio_col, agreement_col]].dropna()
    if len(valid) < 2:
        fig, ax = plt.subplots(figsize=(8, 5))
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig

    fig, ax = plt.subplots(figsize=(8, 5))
    values = valid[log2_ratio_col].values
    vmin, vmax = np.percentile(values, [1, 99])
    bins = np.linspace(vmin - 0.5, vmax + 0.5, 60)

    hist_params = _HistPlotParams(valid, log2_ratio_col, agreement_col, bins, cfg)
    _draw_level_histograms(ax, hist_params)
    _draw_reference_lines(ax)

    ax.set_xlabel("log₂(遥感面积 / HydroATLAS面积)")
    ax.set_ylabel("数量")
    ax.set_title(title)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.2)

    n = len(valid)
    median_log2 = float(np.median(values))
    ax.text(
        0.02, 0.98, f"n = {n:,}\n中位数 = {median_log2:.3f}",
        transform=ax.transAxes, va="top", fontsize=9,
    )
    fig.tight_layout()
    log.debug("plot_ratio_histogram: n=%d", n)
    return fig


def _draw_lake_subplot(
    ax: plt.Axes,
    hid: int,
    df: pd.DataFrame,
    atlas_km2: float,
    ratio: float,
    show_xlabel: bool,
) -> None:
    """Draw a single lake area subplot with atlas reference line."""
    dates = pd.to_datetime({"year": df["year"], "month": df["month"], "day": 1})
    area_km2 = df["water_area"].values / 1e6

    ax.plot(dates, area_km2, linewidth=0.8, color="steelblue")

    if atlas_km2 > 0:
        ax.axhline(
            atlas_km2, color="red", linestyle="-",
            linewidth=2.0, label="HydroATLAS",
        )

    ratio_str = f"{ratio:.2f}" if not np.isnan(ratio) else "N/A"
    ax.set_title(f"ID {hid} (比值={ratio_str})", fontsize=9)
    ax.tick_params(labelsize=7)
    ax.set_ylabel("km²", fontsize=8)

    if show_xlabel:
        ax.set_xlabel("时间", fontsize=8)


def plot_lake_area_grid(
    lake_data: dict[int, pd.DataFrame],
    atlas_areas: dict[int, float],
    ratio_values: dict[int, float],
    *,
    title: str = "遥感面积差异湖泊抽样",
) -> plt.Figure:
    """Plot 3x4 grid of lake area time series with atlas_area reference lines.

    Args:
        lake_data: hylak_id → DataFrame with [year, month, water_area] (m²).
        atlas_areas: hylak_id → atlas_area (km²).
        ratio_values: hylak_id → ratio_mean for subtitle.
        title: Figure suptitle.

    Returns:
        matplotlib Figure with 3x4 subplot grid.
    """
    setup_chinese_font()

    hylak_ids = list(lake_data.keys())
    n = len(hylak_ids)
    if n == 0:
        fig, _ = plt.subplots(figsize=(20, 12))
        fig.suptitle(title)
        fig.tight_layout()
        return fig

    n_cols = 4
    n_rows = 3
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(20, 12))
    axes_flat = axes.flatten()

    for idx in range(n_rows * n_cols):
        ax = axes_flat[idx]
        if idx >= n:
            ax.set_visible(False)
            continue

        hid = hylak_ids[idx]
        _draw_lake_subplot(
            ax, hid, lake_data[hid],
            atlas_areas.get(hid, 0.0),
            ratio_values.get(hid, float("nan")),
            show_xlabel=(idx >= n - n_cols),
        )

    fig.suptitle(title, fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    log.debug("plot_lake_area_grid: n=%d", n)
    return fig


def _draw_symmetric_ref_lines(ax: plt.Axes) -> None:
    """Draw symmetric reference lines: positive=mean, negative=median."""
    refs = [
        (0, "--", 2.0, "#1565C0", "比值=1"),
        (1, ":", 1.8, "#F9A825", "比值=2"),
        (-1, ":", 1.8, "#F9A825", None),
        (np.log2(5), ":", 1.8, "#F9A825", "比值=5"),
        (-np.log2(5), ":", 1.8, "#F9A825", None),
        (np.log2(10), ":", 1.8, "#F9A825", "比值=10"),
        (-np.log2(10), ":", 1.8, "#F9A825", None),
    ]
    for y, ls, lw, color, lbl in refs:
        ax.axhline(y, color=color, linestyle=ls, linewidth=lw, label=lbl, zorder=15)


def _count_by_bin(
    norm_ratio: np.ndarray,
    bins: list[tuple[float, float]],
) -> list[int]:
    """Count lakes in each normalized ratio bin."""
    return [int(np.sum((norm_ratio >= lo) & (norm_ratio < hi))) for lo, hi in bins]


def _annotate_bin_counts(
    ax: plt.Axes,
    counts: list[int],
    y_positions: list[float],
    x_pos: float,
    n_total: int,
    is_negative: bool = False,
    fontsize: int = 7,
) -> None:
    """Annotate bin counts and percentages at reference lines."""
    y_offset = -0.15 if is_negative else 0.15
    va = "top" if is_negative else "bottom"
    for cnt, y in zip(counts, y_positions):
        pct = cnt / n_total * 100 if n_total > 0 else 0
        ax.text(
            x_pos, y + y_offset,
            f"n={cnt} ({pct:.1f}%)",
            fontsize=fontsize, va=va, ha="right",
            color="#333333",
        )


def _highlight_area_tiers(
    ax: plt.Axes,
    atlas: np.ndarray,
    y_vals: np.ndarray,
    norm_ratio: np.ndarray,
    colors: list[str],
    label_prefix: str,
) -> np.ndarray:
    """Highlight smaller-area 50%, 80%, 95% of lakes with norm_ratio >= 5.

    Returns:
        Array of indices for 95% tier (smallest-area 95% of extreme lakes).
    """
    extreme_mask = norm_ratio >= 5
    if not np.any(extreme_mask):
        return np.array([], dtype=int)

    extreme_idx = np.where(extreme_mask)[0]
    sorted_by_area = extreme_idx[np.argsort(atlas[extreme_idx])]
    n_extreme = len(sorted_by_area)

    tiers = [(0.95, 0.3), (0.8, 0.5), (0.5, 0.8)]
    highlight_95pct = np.array([], dtype=int)

    for (pct, alpha), color in zip(tiers, colors):
        n_highlight = max(int(n_extreme * pct), 1)
        highlight = sorted_by_area[:n_highlight]
        ax.scatter(
            atlas[highlight], y_vals[highlight],
            c=color, s=20, alpha=alpha,
            label=f"{label_prefix}（面积较小{int(pct * 100)}%）",
            zorder=10, rasterized=True,
        )
        if pct == 0.95:
            highlight_95pct = highlight

    return highlight_95pct


def plot_area_ratio_distribution(
    df: pd.DataFrame,
    *,
    atlas_col: str = "atlas_area",
    ratio_mean_col: str = "ratio_mean",
    ratio_median_col: str = "ratio_median",
    title: str = "湖泊面积与差异比例分布",
) -> tuple[plt.Figure, dict[str, np.ndarray]]:
    """Scatter plot of atlas_area vs normalized ratio, symmetric layout.

    Upper half (y>0): mean ratio normalized to >=1, log2 scale.
    Lower half (y<0): median ratio normalized to >=1, log2 scale, mirrored.
    Both sides share the same tick labels (1, 2, 5, 10).
    Lakes with norm_ratio >= 5 and in the smaller-area 95% are highlighted.

    Args:
        df: DataFrame with atlas_area, ratio_mean, ratio_median columns.
        atlas_col: Column name for atlas reference area (km²).
        ratio_mean_col: Column name for mean area ratio.
        ratio_median_col: Column name for median area ratio.
        title: Figure title.

    Returns:
        Tuple of (Figure, dict with 'mean_95pct' and 'median_95pct' indices).
    """
    setup_chinese_font()

    cols = [atlas_col, ratio_mean_col, ratio_median_col]
    valid = df[cols].dropna()
    valid = valid[
        (valid[atlas_col] > 0)
        & (valid[ratio_mean_col] > 0)
        & (valid[ratio_median_col] > 0)
    ]
    if len(valid) < 2:
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        fig.tight_layout()
        return fig, {"mean_95pct": np.array([], dtype=int), "median_95pct": np.array([], dtype=int)}

    atlas = valid[atlas_col].values
    ratio_mean_raw = valid[ratio_mean_col].values
    ratio_median_raw = valid[ratio_median_col].values

    mean_norm = np.maximum(ratio_mean_raw, 1.0 / ratio_mean_raw)
    median_norm = np.maximum(ratio_median_raw, 1.0 / ratio_median_raw)

    y_mean = np.log2(mean_norm)
    y_median = -np.log2(median_norm)

    fig, ax = plt.subplots(figsize=(10, 8))

    ax.scatter(
        atlas, y_mean,
        c="#FFCDD2", s=8, alpha=0.5,
        label="均值比值", rasterized=True,
    )
    ax.scatter(
        atlas, y_median,
        c="#BBDEFB", s=8, alpha=0.5,
        label="中位数比值", rasterized=True,
    )

    mean_95pct = _highlight_area_tiers(
        ax, atlas, y_mean, mean_norm,
        colors=["#EF9A9A", "#E57373", "#C62828"],
        label_prefix="均值≥5倍",
    )
    median_95pct = _highlight_area_tiers(
        ax, atlas, y_median, median_norm,
        colors=["#90CAF9", "#42A5F5", "#0D47A1"],
        label_prefix="中位数≥5倍",
    )

    _draw_symmetric_ref_lines(ax)

    n_total = len(valid)
    ratio_bins = [(1, 2), (2, 5), (5, 10), (10, np.inf)]
    mean_counts = _count_by_bin(mean_norm, ratio_bins)
    median_counts = _count_by_bin(median_norm, ratio_bins)

    ref_y_pos = [1, np.log2(5), np.log2(10)]
    x_annot = atlas.max() * 0.7
    _annotate_bin_counts(ax, mean_counts[1:], ref_y_pos, x_annot, n_total, is_negative=False)
    _annotate_bin_counts(
        ax, median_counts[1:], [-y for y in ref_y_pos], x_annot, n_total,
        is_negative=True,
    )

    yticks = [
        -np.log2(10), -np.log2(5), -1, 0,
        1, np.log2(5), np.log2(10),
    ]
    yticklabels = [
        "10", "5", "2", "1",
        "2", "5", "10",
    ]
    ax.set_yticks(yticks)
    ax.set_yticklabels(yticklabels)

    ax.set_xscale("log")
    ax.set_xlabel("HydroATLAS面积 (km²)")
    ax.set_ylabel("偏离倍数（上方=均值，下方=中位数）")
    ax.set_title(title)
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(alpha=0.2, which="both")

    log.debug("plot_area_ratio_distribution: n=%d", n_total)
    return fig, {"mean_95pct": mean_95pct, "median_95pct": median_95pct}
