"""Domain-level draw functions — Interpolation detection visualization."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.presets import (
    INTERP_BASE_LINE,
    INTERP_FLAT_LINE,
)

LINEAR_COLORS = ["#e41a1c", "#ff7f00", "#984ea3", "#4daf4a", "#377eb8"]


def _format_diff(val: float) -> str:
    if abs(val - round(val)) < 0.01:
        return f"{val:,.0f}"
    return f"{val:,.2f}"


def draw_interpolation_timeline(
    ax: plt.Axes,
    series_df: pd.DataFrame,
    segments: list[dict],
    *,
    hylak_id: int | None = None,
    base_style: LineStyle = INTERP_BASE_LINE,
    flat_style: LineStyle = INTERP_FLAT_LINE,
) -> None:
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date").reset_index(drop=True)

    draw_line(ax, line_df["date"], line_df["water_area"], style=base_style)

    dates = line_df["date"].to_numpy()
    areas = line_df["water_area"].to_numpy()

    linear_idx = 0
    for seg in segments:
        start_idx = seg["start_idx"]
        end_idx = seg["end_idx"]
        is_flat = seg["is_flat"]
        diff_val = seg.get("diff_value", 0.0)

        seg_dates = dates[start_idx : end_idx + 1]
        seg_areas = areas[start_idx : end_idx + 1]

        if is_flat:
            ax.plot(seg_dates, seg_areas, color=flat_style.color, linewidth=flat_style.linewidth, zorder=flat_style.zorder, label="flat")
        else:
            color = LINEAR_COLORS[linear_idx % len(LINEAR_COLORS)]
            label = f"linear#{linear_idx + 1} Δ={_format_diff(diff_val)}"
            ax.plot(seg_dates, seg_areas, color=color, linewidth=2.5, zorder=3, label=label)
            linear_idx += 1

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    apply_axis_style(
        ax,
        AxisStyle(
            title=f"Lake {title_suffix}",
            xlabel="",
            ylabel="",
        ),
    )
    ax.legend(loc="best", fontsize=6)
    fig = ax.get_figure()
    fig.autofmt_xdate()

    linear_segs = [s for s in segments if not s["is_flat"]]
    if not linear_segs:
        return
    longest = max(linear_segs, key=lambda s: s["end_idx"] - s["start_idx"])
    si, ei = longest["start_idx"], longest["end_idx"]
    diff_val = longest.get("diff_value", 0.0)
    seg_len = longest.get("length", ei - si + 1)
    longest_color = LINEAR_COLORS[0]

    for idx, s in enumerate(linear_segs):
        if s is longest:
            longest_color = LINEAR_COLORS[idx % len(LINEAR_COLORS)]
            break

    pad_left = max(0, si - 2)
    pad_right = min(len(dates) - 1, ei + 2)

    inset = ax.inset_axes([0.08, 0.08, 0.45, 0.43])
    inset.plot(dates[pad_left:pad_right + 1], areas[pad_left:pad_right + 1],
               color=base_style.color, linewidth=0.8, zorder=1)
    inset.plot(dates[si:ei + 1], areas[si:ei + 1],
               color=longest_color, linewidth=2.5, zorder=3,
               marker="o", markersize=4)
    inset.tick_params(labelsize=5)
    inset.set_title(f"Δ={_format_diff(diff_val)}  n={seg_len}", fontsize=6, pad=2)
    inset.set_xlim(dates[pad_left] - pd.Timedelta(days=15),
                   dates[pad_right] + pd.Timedelta(days=15))
    seg_area_min = areas[si:ei + 1].min()
    seg_area_max = areas[si:ei + 1].max()
    y_margin = max((seg_area_max - seg_area_min) * 0.3, 1.0)
    inset.set_ylim(seg_area_min - y_margin, seg_area_max + y_margin)

    rect = plt.Rectangle(
        (dates[si], seg_area_min - y_margin),
        dates[ei] - dates[si],
        (seg_area_max + y_margin) - (seg_area_min - y_margin),
        linewidth=0.8, edgecolor="gray", facecolor="none", linestyle="--",
    )
    ax.add_patch(rect)


def plot_interpolation_timeline(
    series_df: pd.DataFrame,
    segments: list[dict],
    *,
    hylak_id: int | None = None,
) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 5))
    draw_interpolation_timeline(ax, series_df, segments, hylak_id=hylak_id)
    fig.tight_layout()
    return fig
