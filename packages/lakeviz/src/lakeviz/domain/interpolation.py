"""Domain-level draw functions — Interpolation detection visualization."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.presets import (
    INTERP_BASE_LINE,
    INTERP_LINEAR_LINE,
    INTERP_FLAT_LINE,
)


def draw_interpolation_timeline(
    ax: plt.Axes,
    series_df: pd.DataFrame,
    segments: list[dict],
    *,
    hylak_id: int | None = None,
    base_style: LineStyle = INTERP_BASE_LINE,
    linear_style: LineStyle = INTERP_LINEAR_LINE,
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

    has_linear = False
    has_flat = False

    for seg in segments:
        start_idx = seg["start_idx"]
        end_idx = seg["end_idx"]
        is_flat = seg["is_flat"]

        seg_dates = dates[start_idx : end_idx + 1]
        seg_areas = areas[start_idx : end_idx + 1]

        if is_flat:
            ax.plot(seg_dates, seg_areas, color=flat_style.color, linewidth=flat_style.linewidth, zorder=flat_style.zorder)
            has_flat = True
        else:
            ax.plot(seg_dates, seg_areas, color=linear_style.color, linewidth=linear_style.linewidth, zorder=linear_style.zorder)
            has_linear = True

    if has_linear:
        ax.plot([], [], color=linear_style.color, linewidth=linear_style.linewidth, label="true-linear")
    if has_flat:
        ax.plot([], [], color=flat_style.color, linewidth=flat_style.linewidth, label="flat")

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    apply_axis_style(
        ax,
        AxisStyle(
            title=f"Lake {title_suffix} Interpolation Timeline",
            xlabel="Time",
            ylabel="Water area",
        ),
    )
    ax.legend(loc="best")
    fig = ax.get_figure()
    fig.autofmt_xdate()


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
