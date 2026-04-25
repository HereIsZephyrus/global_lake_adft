"""Domain-level draw functions — Quantile monthly anomaly transitions."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.draw.line import draw_line
from lakeviz.draw.scatter import draw_scatter
from lakeviz.draw.bar import draw_bar
from lakeviz.draw.reference import draw_axhline, draw_axvline
from lakeviz.draw.annotate import draw_annotate_point
from lakeviz.style.base import AxisStyle, apply_axis_style
from lakeviz.style.line import LineStyle
from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.bar import BarStyle
from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.presets import (
    QUANTILE_WATER_AREA,
    QUANTILE_CLIMATOLOGY,
    QUANTILE_EXTREME_HIGH,
    QUANTILE_EXTREME_LOW,
    QUANTILE_TRANSITION_L2H,
    QUANTILE_TRANSITION_H2L,
    ADFT_LINE,
    ADFT_D_TO_W,
    ADFT_W_TO_D,
)


def draw_monthly_timeline(
    ax: plt.Axes,
    labels_df: pd.DataFrame,
    transitions_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    water_area_style: LineStyle = QUANTILE_WATER_AREA,
    climatology_style: LineStyle = QUANTILE_CLIMATOLOGY,
    high_style: ScatterStyle = QUANTILE_EXTREME_HIGH,
    low_style: ScatterStyle = QUANTILE_EXTREME_LOW,
    l2h_style: ReferenceLineStyle = QUANTILE_TRANSITION_L2H,
    h2l_style: ReferenceLineStyle = QUANTILE_TRANSITION_H2L,
) -> None:
    dates = pd.to_datetime(dict(year=labels_df["year"], month=labels_df["month"], day=1))
    draw_line(ax, dates, labels_df["water_area"], style=water_area_style)
    draw_line(ax, dates, labels_df["monthly_climatology"], style=climatology_style)

    high_df = labels_df.loc[labels_df["extreme_label"] == "extreme_high"]
    low_df = labels_df.loc[labels_df["extreme_label"] == "extreme_low"]
    if not high_df.empty:
        high_dates = pd.to_datetime(dict(year=high_df["year"], month=high_df["month"], day=1))
        draw_scatter(ax, high_dates, high_df["water_area"], style=high_style)
    if not low_df.empty:
        low_dates = pd.to_datetime(dict(year=low_df["year"], month=low_df["month"], day=1))
        draw_scatter(ax, low_dates, low_df["water_area"], style=low_style)

    if not transitions_df.empty:
        for transition_type, ref_style in (("low_to_high", l2h_style), ("high_to_low", h2l_style)):
            subset = transitions_df.loc[transitions_df["transition_type"] == transition_type]
            if subset.empty:
                continue
            transition_dates = pd.to_datetime(dict(year=subset["to_year"], month=subset["to_month"], day=1))
            for transition_date in transition_dates:
                draw_axvline(ax, transition_date, style=ref_style)

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    apply_axis_style(ax, AxisStyle(title=f"Lake {title_suffix} Monthly Timeline", xlabel="Month", ylabel="Water area"))
    ax.legend(loc="best")
    fig = ax.get_figure()
    fig.autofmt_xdate()


def draw_anomaly_timeline(
    ax: plt.Axes,
    labels_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    anomaly_style: LineStyle = LineStyle(color="tab:gray", linewidth=1.3, label="anomaly"),
    q_low_style: ReferenceLineStyle = ReferenceLineStyle(color="tab:blue", linestyle="--", label="q_low"),
    q_high_style: ReferenceLineStyle = ReferenceLineStyle(color="tab:red", linestyle="--", label="q_high"),
    high_style: ScatterStyle = QUANTILE_EXTREME_HIGH,
    low_style: ScatterStyle = QUANTILE_EXTREME_LOW,
) -> None:
    dates = pd.to_datetime(dict(year=labels_df["year"], month=labels_df["month"], day=1))
    draw_line(ax, dates, labels_df["anomaly"], style=anomaly_style)
    draw_axhline(ax, 0.0, style=ReferenceLineStyle(color="black", linewidth=0.8))
    draw_axhline(ax, labels_df["q_low"].iloc[0], style=q_low_style)
    draw_axhline(ax, labels_df["q_high"].iloc[0], style=q_high_style)

    high_df = labels_df.loc[labels_df["extreme_label"] == "extreme_high"]
    low_df = labels_df.loc[labels_df["extreme_label"] == "extreme_low"]
    if not high_df.empty:
        high_dates = pd.to_datetime(dict(year=high_df["year"], month=high_df["month"], day=1))
        draw_scatter(ax, high_dates, high_df["anomaly"], style=high_style)
    if not low_df.empty:
        low_dates = pd.to_datetime(dict(year=low_df["year"], month=low_df["month"], day=1))
        draw_scatter(ax, low_dates, low_df["anomaly"], style=low_style)

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    apply_axis_style(ax, AxisStyle(title=f"Lake {title_suffix} Anomaly Timeline", xlabel="Month", ylabel="Anomaly"))
    ax.legend(loc="best")
    fig = ax.get_figure()
    fig.autofmt_xdate()


def draw_transition_count_summary(
    ax: plt.Axes,
    transitions_df: pd.DataFrame,
    *,
    axis_style: AxisStyle = AxisStyle(title="Abrupt Transition Counts", xlabel="Transition type", ylabel="Count"),
) -> None:
    counts = (
        transitions_df["transition_type"]
        .value_counts()
        .reindex(["low_to_high", "high_to_low"], fill_value=0)
    )
    draw_bar(ax, counts.index.tolist(), counts.to_numpy(), style=BarStyle(), colors=["tab:green", "tab:orange"])
    apply_axis_style(ax, axis_style)


def draw_transition_count_summary_from_cache(
    ax: plt.Axes,
    counts_df: pd.DataFrame,
    *,
    axis_style: AxisStyle = AxisStyle(title="Abrupt Transition Counts", xlabel="Transition type", ylabel="Count"),
) -> None:
    counts = (
        counts_df.set_index("transition_type")["count"]
        .reindex(["low_to_high", "high_to_low"], fill_value=0)
    )
    draw_bar(ax, counts.index.tolist(), counts.to_numpy(), style=BarStyle(), colors=["tab:green", "tab:orange"])
    apply_axis_style(ax, axis_style)


def draw_transition_seasonality_summary(
    ax: plt.Axes,
    transitions_df: pd.DataFrame,
    *,
    axis_style: AxisStyle = AxisStyle(title="Abrupt Transition Seasonality", xlabel="Destination month", ylabel="Count"),
) -> None:
    counts = (
        transitions_df["to_month"].value_counts().sort_index().reindex(range(1, 13), fill_value=0)
        if not transitions_df.empty
        else pd.Series(0, index=range(1, 13))
    )
    draw_bar(ax, counts.index.tolist(), counts.to_numpy(), style=BarStyle(color="tab:purple"))
    ax.set_xticks(range(1, 13))
    apply_axis_style(ax, axis_style)


def draw_transition_seasonality_summary_from_cache(
    ax: plt.Axes,
    seasonality_df: pd.DataFrame,
    *,
    axis_style: AxisStyle = AxisStyle(title="Abrupt Transition Seasonality", xlabel="Destination month", ylabel="Count"),
) -> None:
    month_column = "month" if "month" in seasonality_df.columns else "to_month"
    counts = (
        seasonality_df.set_index(month_column)["count"]
        .reindex(range(1, 13), fill_value=0)
    )
    draw_bar(ax, counts.index.tolist(), counts.to_numpy(), style=BarStyle(color="tab:purple"))
    ax.set_xticks(range(1, 13))
    apply_axis_style(ax, axis_style)


def draw_adft_fallback(
    ax: plt.Axes,
    hylak_id: int,
    series_df: pd.DataFrame,
    adft_df: pd.DataFrame,
) -> None:
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date")

    date_to_area: dict[tuple[int, int], float] = {}
    for _, row in line_df.iterrows():
        date_to_area[(int(row["year"]), int(row["month"]))] = float(row["water_area"])

    draw_line(ax, line_df["date"], line_df["water_area"], style=ADFT_LINE)

    drought_to_flood_segments: list[dict] = []
    flood_to_drought_segments: list[dict] = []

    if adft_df is not None and not adft_df.empty:
        for _, row in adft_df.iterrows():
            y, m = int(row["year"]), int(row["month"])
            prev_date = pd.Timestamp(year=y, month=m, day=1)
            next_date = prev_date + pd.DateOffset(months=1)
            prev_key = (y, m)
            next_key = (next_date.year, next_date.month)
            if prev_key in date_to_area and next_key in date_to_area:
                segment = {
                    "dates": [prev_date, next_date],
                    "areas": [date_to_area[prev_key], date_to_area[next_key]],
                }
                if bool(row["is_drought_to_flood"]):
                    drought_to_flood_segments.append(segment)
                else:
                    flood_to_drought_segments.append(segment)

    for segment in flood_to_drought_segments:
        ax.plot(segment["dates"], segment["areas"], color="#D2691E", linewidth=4, zorder=3, alpha=1.0)
        mid_date = segment["dates"][0]
        mid_area = segment["areas"][0]
        date_label = mid_date.strftime("%Y-%m")
        ax.text(mid_date, mid_area, f"  {date_label}\n  涝→旱", fontsize=8, color="#D2691E", fontweight="bold", ha="center", va="bottom", zorder=5, bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#D2691E", alpha=0.8))

    for segment in drought_to_flood_segments:
        ax.plot(segment["dates"], segment["areas"], color="#8B008B", linewidth=4, zorder=3, alpha=1.0)
        mid_date = segment["dates"][0]
        mid_area = segment["areas"][0]
        date_label = mid_date.strftime("%Y-%m")
        ax.text(mid_date, mid_area, f"  {date_label}\n  旱→涝", fontsize=8, color="#8B008B", fontweight="bold", ha="center", va="bottom", zorder=5, bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#8B008B", alpha=0.8))

    if flood_to_drought_segments:
        ax.plot([], [], color="#D2691E", linewidth=4, label="涝转旱事件", alpha=1.0)
    if drought_to_flood_segments:
        ax.plot([], [], color="#8B008B", linewidth=4, label="旱转涝事件", alpha=1.0)

    apply_axis_style(ax, AxisStyle(
        xlabel="时间 (Year-Month)", ylabel="水域面积 (km²)",
        title=f"湖泊 {hylak_id} 面积变化时序图（含ADFT事件）",
    )._replace(grid_alpha=0.3, grid_linestyle="--"))
    ax.legend(loc="best", fontsize=11, framealpha=0.9)
    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(plt.matplotlib.dates.YearLocator())
    plt.xticks(rotation=45)


# ---------------------------------------------------------------------------
# Backward-compatible convenience wrappers
# ---------------------------------------------------------------------------

def plot_monthly_timeline(labels_df, transitions_df, *, hylak_id=None) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 5))
    draw_monthly_timeline(ax, labels_df, transitions_df, hylak_id=hylak_id)
    fig.tight_layout()
    return fig


def plot_anomaly_timeline(labels_df, *, hylak_id=None) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(12, 5))
    draw_anomaly_timeline(ax, labels_df, hylak_id=hylak_id)
    fig.tight_layout()
    return fig


def plot_transition_count_summary(transitions_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_transition_count_summary(ax, transitions_df)
    fig.tight_layout()
    return fig


def plot_transition_count_summary_from_cache(counts_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(8, 5))
    draw_transition_count_summary_from_cache(ax, counts_df)
    fig.tight_layout()
    return fig


def plot_transition_seasonality_summary(transitions_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 5))
    draw_transition_seasonality_summary(ax, transitions_df)
    fig.tight_layout()
    return fig


def plot_transition_seasonality_summary_from_cache(seasonality_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(10, 5))
    draw_transition_seasonality_summary_from_cache(ax, seasonality_df)
    fig.tight_layout()
    return fig


def plot_adft_fallback(hylak_id, series_df, adft_df) -> plt.Figure:
    fig, ax = plt.subplots(figsize=(14, 7))
    draw_adft_fallback(ax, hylak_id, series_df, adft_df)
    fig.tight_layout()
    return fig