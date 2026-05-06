"""Domain-level draw functions — Interpolation detection visualization."""

from __future__ import annotations

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates
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


def _prepare_timeline_data(series_df: pd.DataFrame):
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date").reset_index(drop=True)
    return line_df


def _draw_segments(
    ax: plt.Axes,
    dates: pd.Series,
    areas: pd.Series,
    segments: list[dict],
    *,
    base_style: LineStyle = INTERP_BASE_LINE,
    flat_style: LineStyle = INTERP_FLAT_LINE,
    legend_fontsize: int = 6,
    legend_loc: str = "best",
) -> None:
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
    ax.legend(loc=legend_loc, fontsize=legend_fontsize)


def _find_longest_linear(segments: list[dict]) -> dict | None:
    linear_segs = [s for s in segments if not s["is_flat"]]
    if not linear_segs:
        return None
    return max(linear_segs, key=lambda s: s["end_idx"] - s["start_idx"])


def _longest_linear_color(segments: list[dict], longest: dict) -> str:
    linear_segs = [s for s in segments if not s["is_flat"]]
    for idx, s in enumerate(linear_segs):
        if s is longest:
            return LINEAR_COLORS[idx % len(LINEAR_COLORS)]
    return LINEAR_COLORS[0]


def draw_interpolation_timeline(
    ax: plt.Axes,
    series_df: pd.DataFrame,
    segments: list[dict],
    *,
    hylak_id: int | None = None,
    base_style: LineStyle = INTERP_BASE_LINE,
    flat_style: LineStyle = INTERP_FLAT_LINE,
) -> None:
    line_df = _prepare_timeline_data(series_df)
    draw_line(ax, line_df["date"], line_df["water_area"], style=base_style)

    dates = line_df["date"].to_numpy()
    areas = line_df["water_area"].to_numpy()

    _draw_segments(ax, dates, areas, segments, base_style=base_style, flat_style=flat_style)

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    apply_axis_style(ax, AxisStyle(title=f"Lake {title_suffix}", xlabel="", ylabel=""))

    fig = ax.get_figure()
    fig.autofmt_xdate()

    longest = _find_longest_linear(segments)
    if longest is None:
        return

    si, ei = longest["start_idx"], longest["end_idx"]
    diff_val = longest.get("diff_value", 0.0)
    seg_len = longest.get("length", ei - si + 1)
    longest_color = _longest_linear_color(segments, longest)

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


def draw_interpolation_timeline_hq_main(
    ax: plt.Axes,
    series_df: pd.DataFrame,
    segments: list[dict],
    *,
    hylak_id: int | None = None,
    base_style: LineStyle = INTERP_BASE_LINE,
    flat_style: LineStyle = INTERP_FLAT_LINE,
) -> None:
    line_df = _prepare_timeline_data(series_df)
    draw_line(ax, line_df["date"], line_df["water_area"], style=base_style)

    dates = line_df["date"].to_numpy()
    areas = line_df["water_area"].to_numpy()

    _draw_segments(ax, dates, areas, segments, base_style=base_style, flat_style=flat_style,
                   legend_fontsize=10, legend_loc="upper right")

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    apply_axis_style(ax, AxisStyle(title=f"Lake {title_suffix}", xlabel="", ylabel=""))
    ax.title.set_fontsize(14)

    linear_segs = [s for s in segments if not s["is_flat"]]
    for idx, seg in enumerate(linear_segs):
        si, ei = seg["start_idx"], seg["end_idx"]
        seg_area_min = areas[si:ei + 1].min()
        seg_area_max = areas[si:ei + 1].max()
        y_margin = max((seg_area_max - seg_area_min) * 0.3, 1.0)
        color = LINEAR_COLORS[idx % len(LINEAR_COLORS)]
        rect = plt.Rectangle(
            (dates[si], seg_area_min - y_margin),
            dates[ei] - dates[si],
            (seg_area_max + y_margin) - (seg_area_min - y_margin),
            linewidth=0.8, edgecolor=color, facecolor="none", linestyle="--",
        )
        ax.add_patch(rect)


def draw_interpolation_timeline_hq_inset(
    ax: plt.Axes,
    series_df: pd.DataFrame,
    segment: dict,
    *,
    color_idx: int = 0,
    base_style: LineStyle = INTERP_BASE_LINE,
) -> None:
    line_df = _prepare_timeline_data(series_df)

    dates = line_df["date"].to_numpy()
    areas = line_df["water_area"].to_numpy()

    si, ei = segment["start_idx"], segment["end_idx"]
    diff_val = segment.get("diff_value", 0.0)
    seg_len = segment.get("length", ei - si + 1)
    seg_color = LINEAR_COLORS[color_idx % len(LINEAR_COLORS)]

    pad_left = max(0, si - 2)
    pad_right = min(len(dates) - 1, ei + 2)

    ax.plot(dates[pad_left:pad_right + 1], areas[pad_left:pad_right + 1],
            color=base_style.color, linewidth=0.8, zorder=1)
    ax.plot(dates[si:ei + 1], areas[si:ei + 1],
            color=seg_color, linewidth=2.5, zorder=3,
            marker="o", markersize=4)
    ax.tick_params(labelsize=9)
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("%m"))
    ax.set_title(f"Δ={_format_diff(diff_val)}  n={seg_len}", fontsize=11, pad=2)
    ax.set_xlim(dates[pad_left] - pd.Timedelta(days=15),
                dates[pad_right] + pd.Timedelta(days=15))
    seg_area_min = areas[si:ei + 1].min()
    seg_area_max = areas[si:ei + 1].max()
    y_margin = max((seg_area_max - seg_area_min) * 0.3, 1.0)
    ax.set_ylim(seg_area_min - y_margin, seg_area_max + y_margin)
    ax.grid(alpha=0.2, linestyle=":")


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
