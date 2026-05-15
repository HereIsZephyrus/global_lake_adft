"""Domain-level draw functions — area comparison and anomaly plots."""

from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


_AGREEMENT_COLORS = {
    "good": "#2ca02c",
    "moderate": "#ffbb78",
    "poor": "#d62728",
}

_DEFAULT_CONFIG = {"good": 2.0, "moderate": 5.0, "poor": 10.0}

_LEGACY_LEVEL_MAP = {
    "excellent": "good",
    "extreme": "poor",
}

_SET_DISPLAY_NAMES = {
    "is_median_zero": "面积为0",
    "is_flat_or_pv": "序列异常",
    "is_area_mismatch": "面积偏差",
    "is_shift": "结构性变化",
}


def _agreement_label(level: str, config: dict[str, float]) -> str:
    g = config.get("good", 2.0)
    m = config.get("moderate", 5.0)
    p = config.get("poor", 10.0)
    labels = {
        "good": f"{1 / g:.1f}~{g:.0f}倍",
        "moderate": f"{1 / m:.1f}~{m:.0f}倍",
        "poor": f"<{1 / p:.1f} 或 >{p:.0f}倍",
    }
    return labels.get(level, level)


def _normalize_agreement_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df
    out = df.copy()
    out[col] = out[col].map(lambda x: _LEGACY_LEVEL_MAP.get(x, x))
    return out


def draw_area_scatter(
    ax: plt.Axes,
    df: pd.DataFrame,
    *,
    atlas_col: str = "atlas_area",
    rs_col: str = "rs_area_median",
    agreement_col: str = "agreement_median",
    title: str = "遥感面积与HydroATLAS面积对比",
    config: dict[str, float] | None = None,
) -> None:
    cfg = config if config is not None else _DEFAULT_CONFIG
    df = _normalize_agreement_col(df, agreement_col)
    valid = df[[atlas_col, rs_col, agreement_col]].dropna()
    valid = valid[(valid[atlas_col] > 0) & (valid[rs_col] > 0)]
    if len(valid) < 2:
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        return

    for level, color in _AGREEMENT_COLORS.items():
        subset = valid[valid[agreement_col] == level]
        if len(subset) == 0:
            continue
        label = f"{_agreement_label(level, cfg)} ({len(subset):,})"
        ax.scatter(subset[atlas_col], subset[rs_col], c=color, s=8, alpha=0.5, label=label, rasterized=True)

    lo = max(valid[atlas_col].min(), valid[rs_col].min()) * 0.5
    hi = max(valid[atlas_col].max(), valid[rs_col].max()) * 2
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=1.0, label="1:1线", zorder=5)
    ax.plot([lo, hi], [lo * 2, hi * 2], ":", color="grey", linewidth=0.8, label="2倍线", zorder=5)
    ax.plot([lo, hi], [lo / 2, hi / 2], ":", color="grey", linewidth=0.8, zorder=5)
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


def draw_ratio_histogram(
    ax: plt.Axes,
    df: pd.DataFrame,
    *,
    log2_ratio_col: str = "log2_ratio_median",
    agreement_col: str = "agreement_median",
    title: str = "log₂(遥感面积/HydroATLAS面积)分布",
    config: dict[str, float] | None = None,
) -> None:
    cfg = config if config is not None else _DEFAULT_CONFIG
    df = _normalize_agreement_col(df, agreement_col)
    valid = df[[log2_ratio_col, agreement_col]].dropna()
    if len(valid) < 2:
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        return
    values = valid[log2_ratio_col].values
    vmin, vmax = np.percentile(values, [1, 99])
    bins = np.linspace(vmin - 0.5, vmax + 0.5, 60)
    for level, color in _AGREEMENT_COLORS.items():
        subset = valid[valid[agreement_col] == level][log2_ratio_col].values
        if len(subset) == 0:
            continue
        label = f"{_agreement_label(level, cfg)} ({len(subset):,})"
        ax.hist(subset, bins=bins, alpha=0.6, color=color, edgecolor="white", linewidth=0.3, label=label)
    ax.axvline(0, color="black", linestyle="--", linewidth=1.0, label="比值=1")
    ax.axvline(1, color="grey", linestyle=":", linewidth=0.8, label="比值=2")
    ax.axvline(-1, color="grey", linestyle=":", linewidth=0.8, label="比值=0.5")
    n = len(valid)
    median_log2 = float(np.median(values))
    ax.text(0.02, 0.98, f"n = {n:,}\n中位数 = {median_log2:.3f}", transform=ax.transAxes, va="top", fontsize=9)
    ax.set_xlabel("log₂(遥感面积 / HydroATLAS面积)")
    ax.set_ylabel("数量")
    ax.set_title(title)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.2)


def draw_lake_area_grid(
    axes: np.ndarray,
    lake_data: dict[int, pd.DataFrame],
    atlas_areas: dict[int, float],
    ratio_values: dict[int, float],
) -> None:
    hylak_ids = list(lake_data.keys())
    axes_flat = axes.flatten()
    n = len(hylak_ids)
    for idx, ax in enumerate(axes_flat):
        if idx >= n:
            ax.set_visible(False)
            continue
        hid = hylak_ids[idx]
        df = lake_data[hid]
        dates = pd.to_datetime({"year": df["year"], "month": df["month"], "day": 1})
        area_km2 = df["water_area"].values / 1e6
        ax.plot(dates, area_km2, linewidth=0.8, color="steelblue")
        atlas_km2 = atlas_areas.get(hid, 0.0)
        if atlas_km2 > 0:
            ax.axhline(atlas_km2, color="red", linestyle="-", linewidth=2.0, label="HydroATLAS")
        ratio = ratio_values.get(hid, float("nan"))
        ratio_str = f"{ratio:.2f}" if not np.isnan(ratio) else "N/A"
        ax.set_title(f"ID {hid} (比值={ratio_str})", fontsize=9)
        ax.tick_params(labelsize=7)
        ax.set_ylabel("km²", fontsize=8)
        if idx >= max(n - 4, 0):
            ax.set_xlabel("时间", fontsize=8)


def draw_area_ratio_distribution(
    ax: plt.Axes,
    df: pd.DataFrame,
    *,
    atlas_col: str = "atlas_area",
    ratio_mean_col: str = "ratio_mean",
    ratio_median_col: str = "ratio_median",
    title: str = "湖泊面积与差异比例分布",
) -> dict[str, np.ndarray]:
    cols = [atlas_col, ratio_mean_col, ratio_median_col]
    valid = df[cols].dropna()
    valid = valid[(valid[atlas_col] > 0) & (valid[ratio_mean_col] > 0) & (valid[ratio_median_col] > 0)]
    if len(valid) < 2:
        ax.text(0.5, 0.5, "数据不足", ha="center", va="center", transform=ax.transAxes)
        return {"mean_95pct": np.array([], dtype=int), "median_95pct": np.array([], dtype=int)}

    atlas = valid[atlas_col].values
    mean_norm = np.maximum(valid[ratio_mean_col].values, 1.0 / valid[ratio_mean_col].values)
    median_norm = np.maximum(valid[ratio_median_col].values, 1.0 / valid[ratio_median_col].values)
    y_mean = np.log2(mean_norm)
    y_median = -np.log2(median_norm)

    ax.scatter(atlas, y_mean, c="#FFCDD2", s=8, alpha=0.5, label="均值比值", rasterized=True)
    ax.scatter(atlas, y_median, c="#BBDEFB", s=8, alpha=0.5, label="中位数比值", rasterized=True)

    def _highlight(norm_ratio: np.ndarray, y_vals: np.ndarray, colors: list[str], label_prefix: str) -> np.ndarray:
        extreme_idx = np.where(norm_ratio >= 5)[0]
        if len(extreme_idx) == 0:
            return np.array([], dtype=int)
        sorted_by_area = extreme_idx[np.argsort(atlas[extreme_idx])]
        highlight_95pct = np.array([], dtype=int)
        for (pct, alpha), color in zip([(0.95, 0.3), (0.8, 0.5), (0.5, 0.8)], colors, strict=True):
            n_highlight = max(int(len(sorted_by_area) * pct), 1)
            highlight = sorted_by_area[:n_highlight]
            ax.scatter(atlas[highlight], y_vals[highlight], c=color, s=20, alpha=alpha, label=f"{label_prefix}（面积较小{int(pct * 100)}%）", zorder=10, rasterized=True)
            if pct == 0.95:
                highlight_95pct = highlight
        return highlight_95pct

    mean_95pct = _highlight(mean_norm, y_mean, ["#EF9A9A", "#E57373", "#C62828"], "均值≥5倍")
    median_95pct = _highlight(median_norm, y_median, ["#90CAF9", "#42A5F5", "#0D47A1"], "中位数≥5倍")

    for y, ls, lw, color, lbl in [
        (0, "--", 2.0, "#1565C0", "比值=1"),
        (1, ":", 1.8, "#F9A825", "比值=2"),
        (-1, ":", 1.8, "#F9A825", None),
        (np.log2(5), ":", 1.8, "#F9A825", "比值=5"),
        (-np.log2(5), ":", 1.8, "#F9A825", None),
        (np.log2(10), ":", 1.8, "#F9A825", "比值=10"),
        (-np.log2(10), ":", 1.8, "#F9A825", None),
    ]:
        ax.axhline(y, color=color, linestyle=ls, linewidth=lw, label=lbl, zorder=15)

    ratio_bins = [(1, 2), (2, 5), (5, 10), (10, np.inf)]
    mean_counts = [int(np.sum((mean_norm >= lo) & (mean_norm < hi))) for lo, hi in ratio_bins]
    median_counts = [int(np.sum((median_norm >= lo) & (median_norm < hi))) for lo, hi in ratio_bins]
    ref_y_pos = [1, np.log2(5), np.log2(10)]
    x_annot = atlas.max() * 0.7
    n_total = len(valid)
    for counts, ys, is_negative in ((mean_counts[1:], ref_y_pos, False), (median_counts[1:], [-y for y in ref_y_pos], True)):
        y_offset = -0.15 if is_negative else 0.15
        va = "top" if is_negative else "bottom"
        for cnt, y in zip(counts, ys, strict=True):
            pct = cnt / n_total * 100 if n_total > 0 else 0
            ax.text(x_annot, y + y_offset, f"n={cnt} ({pct:.1f}%)", fontsize=7, va=va, ha="right", color="#333333")

    ax.set_yticks([-np.log2(10), -np.log2(5), -1, 0, 1, np.log2(5), np.log2(10)])
    ax.set_yticklabels(["10", "5", "2", "1", "2", "5", "10"])
    ax.set_xscale("log")
    ax.set_xlabel("HydroATLAS面积 (km²)")
    ax.set_ylabel("偏离倍数（上方=均值，下方=中位数）")
    ax.set_title(title)
    ax.legend(fontsize=8, loc="upper right")
    ax.grid(alpha=0.2, which="both")
    return {"mean_95pct": mean_95pct, "median_95pct": median_95pct}


def plot_anomaly_upset(
    flags_df: pd.DataFrame,
    *,
    min_size: int = 0,
    show_counts: bool = True,
    title: str = "异常集合交集",
) -> plt.Figure:
    import upsetplot

    set_cols = list(_SET_DISPLAY_NAMES.keys())
    for col in set_cols:
        if col not in flags_df.columns:
            raise ValueError(f"flags_df missing required column: {col}")

    plot_df = flags_df.copy()
    for col in set_cols:
        plot_df[col] = plot_df[col].astype(bool)
    rename_map = {col: _SET_DISPLAY_NAMES[col] for col in set_cols}
    plot_df = plot_df.rename(columns=rename_map)
    counts = upsetplot.from_indicators(list(rename_map.values()), data=plot_df)
    counts = counts[counts >= min_size]
    fig = plt.figure(figsize=(10, 6))

    # Workaround: upsetplot 0.9.0 uses fillna(..., inplace=True) which is a
    # no-op under pandas >= 3.0 Copy-on-Write, leaving NaN in edgecolor arrays
    # and crashing matplotlib.  Monkeypatch UpSet.plot_matrix to use the
    # equivalent non-inplace fillna assignments.
    # pylint: disable=protected-access
    _original_plot_matrix = upsetplot.UpSet.plot_matrix

    def _patched_plot_matrix(self, ax):
        ax = self._reorient(ax)
        data = self.intersections
        n_cats = data.index.nlevels
        inclusion = data.index.to_frame().values
        styles = [
            [
                self.subset_styles[i]
                if inclusion[i, j]
                else {"facecolor": self._other_dots_color, "linewidth": 0}
                for j in range(n_cats)
            ]
            for i in range(len(data))
        ]
        styles = sum(styles, [])
        style_columns = {
            "facecolor": "facecolors",
            "edgecolor": "edgecolors",
            "linewidth": "linewidths",
            "linestyle": "linestyles",
            "hatch": "hatch",
        }
        styles = (
            pd.DataFrame(styles)
            .reindex(columns=style_columns.keys())
            .astype(
                {
                    "facecolor": "O",
                    "edgecolor": "O",
                    "linewidth": float,
                    "linestyle": "O",
                    "hatch": "O",
                }
            )
        )
        styles["linewidth"] = styles["linewidth"].fillna(1)
        styles["facecolor"] = styles["facecolor"].fillna(self._facecolor)
        styles["edgecolor"] = styles["edgecolor"].fillna(styles["facecolor"])
        styles["linestyle"] = styles["linestyle"].fillna("solid")
        del styles["hatch"]

        x = np.repeat(np.arange(len(data)), n_cats)
        y = np.tile(np.arange(n_cats), len(data))
        if self._element_size is not None:
            s = (self._element_size * 0.35) ** 2
        else:
            s = 200
        ax.scatter(
            *self._swapaxes(x, y),
            s=s,
            zorder=10,
            **styles.rename(columns=style_columns),
        )
        if self._with_lines:
            idx = np.flatnonzero(inclusion)
            line_data = (
                pd.Series(y[idx], index=x[idx])
                .groupby(level=0)
                .aggregate(["min", "max"])
            )
            colors = pd.Series(
                [
                    style.get("edgecolor", style.get("facecolor", self._facecolor))
                    for style in self.subset_styles
                ],
                name="color",
            )
            line_data = line_data.join(colors)
            ax.vlines(
                line_data.index.values,
                line_data["min"],
                line_data["max"],
                colors=line_data["color"],
            )

    upsetplot.UpSet.plot_matrix = _patched_plot_matrix
    try:
        upsetplot.plot(counts, fig=fig, show_counts=show_counts, sort_by="cardinality")
    finally:
        upsetplot.UpSet.plot_matrix = _original_plot_matrix
    # pylint: enable=protected-access
    fig.suptitle(title, fontsize=13)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return fig


def plot_area_scatter(df: pd.DataFrame, **kwargs) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 6))
    draw_area_scatter(ax, df, **kwargs)
    fig.tight_layout()
    return fig


def plot_ratio_histogram(df: pd.DataFrame, **kwargs) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_ratio_histogram(ax, df, **kwargs)
    fig.tight_layout()
    return fig


def plot_lake_area_grid(
    lake_data: dict[int, pd.DataFrame],
    atlas_areas: dict[int, float],
    ratio_values: dict[int, float],
    *,
    title: str = "遥感面积差异湖泊抽样",
) -> plt.Figure:
    if len(lake_data) == 0:
        fig, _ = plt.subplots(figsize=(20, 12))
        fig.suptitle(title)
        fig.tight_layout()
        return fig
    fig, axes = plt.subplots(3, 4, figsize=(20, 12))
    draw_lake_area_grid(axes, lake_data, atlas_areas, ratio_values)
    fig.suptitle(title, fontsize=14)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    return fig


def plot_area_ratio_distribution(df: pd.DataFrame, **kwargs) -> tuple[plt.Figure, dict[str, np.ndarray]]:
    fig, ax = plt.subplots(figsize=(10, 8))
    result = draw_area_ratio_distribution(ax, df, **kwargs)
    return fig, result
