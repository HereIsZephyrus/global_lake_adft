"""Plot helpers for the monthly anomaly transition workflow."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakeviz.plot_config import setup_chinese_font
from lakeviz.layout import save as _save


def plot_monthly_timeline(
    labels_df: pd.DataFrame,
    transitions_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
) -> plt.Figure:
    """Plot water area, climatology, extreme months, and transitions."""
    dates = pd.to_datetime(dict(year=labels_df["year"], month=labels_df["month"], day=1))
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dates, labels_df["water_area"], label="water_area", linewidth=1.5)
    ax.plot(
        dates,
        labels_df["monthly_climatology"],
        label="monthly_climatology",
        linewidth=1.2,
        linestyle="--",
    )

    high_df = labels_df.loc[labels_df["extreme_label"] == "extreme_high"]
    low_df = labels_df.loc[labels_df["extreme_label"] == "extreme_low"]
    if not high_df.empty:
        high_dates = pd.to_datetime(dict(year=high_df["year"], month=high_df["month"], day=1))
        ax.scatter(high_dates, high_df["water_area"], color="tab:red", s=28, label="extreme_high")
    if not low_df.empty:
        low_dates = pd.to_datetime(dict(year=low_df["year"], month=low_df["month"], day=1))
        ax.scatter(low_dates, low_df["water_area"], color="tab:blue", s=28, label="extreme_low")

    if not transitions_df.empty:
        for transition_type, color in (
            ("low_to_high", "tab:green"),
            ("high_to_low", "tab:orange"),
        ):
            subset = transitions_df.loc[transitions_df["transition_type"] == transition_type]
            if subset.empty:
                continue
            transition_dates = pd.to_datetime(
                dict(year=subset["to_year"], month=subset["to_month"], day=1)
            )
            for transition_date in transition_dates:
                ax.axvline(transition_date, color=color, linestyle=":", linewidth=0.9, alpha=0.8)

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    ax.set_title(f"Lake {title_suffix} Monthly Timeline")
    ax.set_xlabel("Month")
    ax.set_ylabel("Water area")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def plot_anomaly_timeline(labels_df: pd.DataFrame, *, hylak_id: int | None = None) -> plt.Figure:
    """Plot anomalies and lake-relative quantile thresholds."""
    dates = pd.to_datetime(dict(year=labels_df["year"], month=labels_df["month"], day=1))
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(dates, labels_df["anomaly"], color="tab:gray", linewidth=1.3, label="anomaly")
    ax.axhline(0.0, color="black", linewidth=0.8)
    ax.axhline(labels_df["q_low"].iloc[0], color="tab:blue", linestyle="--", label="q_low")
    ax.axhline(labels_df["q_high"].iloc[0], color="tab:red", linestyle="--", label="q_high")

    high_df = labels_df.loc[labels_df["extreme_label"] == "extreme_high"]
    low_df = labels_df.loc[labels_df["extreme_label"] == "extreme_low"]
    if not high_df.empty:
        high_dates = pd.to_datetime(dict(year=high_df["year"], month=high_df["month"], day=1))
        ax.scatter(high_dates, high_df["anomaly"], color="tab:red", s=28, label="extreme_high")
    if not low_df.empty:
        low_dates = pd.to_datetime(dict(year=low_df["year"], month=low_df["month"], day=1))
        ax.scatter(low_dates, low_df["anomaly"], color="tab:blue", s=28, label="extreme_low")

    title_suffix = "unknown" if hylak_id is None else str(hylak_id)
    ax.set_title(f"Lake {title_suffix} Anomaly Timeline")
    ax.set_xlabel("Month")
    ax.set_ylabel("Anomaly")
    ax.legend(loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()
    return fig


def plot_transition_count_summary(transitions_df: pd.DataFrame) -> plt.Figure:
    """Plot transition counts by direction."""
    counts = (
        transitions_df["transition_type"]
        .value_counts()
        .reindex(["low_to_high", "high_to_low"], fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(counts.index, counts.to_numpy(), color=["tab:green", "tab:orange"])
    ax.set_title("Abrupt Transition Counts")
    ax.set_xlabel("Transition type")
    ax.set_ylabel("Count")
    fig.tight_layout()
    return fig


def plot_transition_count_summary_from_cache(counts_df: pd.DataFrame) -> plt.Figure:
    """Plot transition counts from cached aggregated counts."""
    counts = (
        counts_df.set_index("transition_type")["count"]
        .reindex(["low_to_high", "high_to_low"], fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(counts.index, counts.to_numpy(), color=["tab:green", "tab:orange"])
    ax.set_title("Abrupt Transition Counts")
    ax.set_xlabel("Transition type")
    ax.set_ylabel("Count")
    fig.tight_layout()
    return fig


def plot_transition_seasonality_summary(transitions_df: pd.DataFrame) -> plt.Figure:
    """Plot transition counts by destination month."""
    counts = (
        transitions_df["to_month"].value_counts().sort_index().reindex(range(1, 13), fill_value=0)
        if not transitions_df.empty
        else pd.Series(0, index=range(1, 13))
    )
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(counts.index, counts.to_numpy(), color="tab:purple")
    ax.set_xticks(range(1, 13))
    ax.set_title("Abrupt Transition Seasonality")
    ax.set_xlabel("Destination month")
    ax.set_ylabel("Count")
    fig.tight_layout()
    return fig


def plot_transition_seasonality_summary_from_cache(seasonality_df: pd.DataFrame) -> plt.Figure:
    """Plot transition seasonality from cached aggregated counts."""
    month_column = "month" if "month" in seasonality_df.columns else "to_month"
    counts = (
        seasonality_df.set_index(month_column)["count"]
        .reindex(range(1, 13), fill_value=0)
    )
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(counts.index, counts.to_numpy(), color="tab:purple")
    ax.set_xticks(range(1, 13))
    ax.set_title("Abrupt Transition Seasonality")
    ax.set_xlabel("Destination month")
    ax.set_ylabel("Count")
    fig.tight_layout()
    return fig


def save_lake_plots(
    labels_df: pd.DataFrame,
    transitions_df: pd.DataFrame,
    output_root: Path,
    *,
    hylak_id: int | None = None,
) -> dict[str, Path]:
    """Save the two required single-lake plots."""
    setup_chinese_font()
    lake_name = "unknown" if hylak_id is None else str(hylak_id)
    output_dir = output_root / "lakes" / lake_name
    output_dir.mkdir(parents=True, exist_ok=True)

    monthly_path = output_dir / "monthly_timeline.png"
    anomaly_path = output_dir / "anomaly_timeline.png"

    monthly_fig = plot_monthly_timeline(labels_df, transitions_df, hylak_id=hylak_id)
    _save(monthly_fig, monthly_path)

    anomaly_fig = plot_anomaly_timeline(labels_df, hylak_id=hylak_id)
    _save(anomaly_fig, anomaly_path)

    return {
        "monthly_timeline": monthly_path,
        "anomaly_timeline": anomaly_path,
    }


def save_summary_plots(transitions_df: pd.DataFrame, output_root: Path) -> dict[str, Path]:
    """Save the required cross-lake summary plots."""
    setup_chinese_font()
    output_dir = output_root / "summary"
    output_dir.mkdir(parents=True, exist_ok=True)

    count_path = output_dir / "transition_count_summary.png"
    seasonality_path = output_dir / "transition_seasonality.png"

    count_fig = plot_transition_count_summary(transitions_df)
    _save(count_fig, count_path)

    seasonality_fig = plot_transition_seasonality_summary(transitions_df)
    _save(seasonality_fig, seasonality_path)

    return {
        "transition_count_summary": count_path,
        "transition_seasonality": seasonality_path,
    }


def plot_adft_fallback(
    hylak_id: int,
    series_df: pd.DataFrame,
    adft_df: pd.DataFrame,
) -> plt.Figure:
    """Plot monthly series with ADFT (Abrupt Drought-Flood Transition) event segments.

    This is the fallback chart for lakes in the area_anomalies table that
    lack Hawkes results.  It draws thick coloured line segments for each
    drought-to-flood or flood-to-drought transition.

    Parameters
    ----------
    hylak_id: Lake identifier (title only).
    series_df: columns [year, month, water_area]
    adft_df: columns [year, month, is_drought_to_flood]
        Each row represents a transition event.  *is_drought_to_flood* is
        True for drought→flood and False for flood→drought.
    """
    line_df = series_df.loc[:, ["year", "month", "water_area"]].dropna().copy()
    line_df["year"] = line_df["year"].astype(int)
    line_df["month"] = line_df["month"].astype(int)
    line_df["date"] = pd.to_datetime(dict(year=line_df["year"], month=line_df["month"], day=1))
    line_df = line_df.sort_values("date")

    date_to_area: dict[tuple[int, int], float] = {}
    for _, row in line_df.iterrows():
        date_to_area[(int(row["year"]), int(row["month"]))] = float(row["water_area"])

    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(line_df["date"], line_df["water_area"], linewidth=2, color="steelblue", marker="o", markersize=3, label="水域面积", zorder=1, alpha=0.8)

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

    ax.set_xlabel("时间 (Year-Month)", fontsize=12)
    ax.set_ylabel("水域面积 (km²)", fontsize=12)
    ax.set_title(f"湖泊 {hylak_id} 面积变化时序图（含ADFT事件）", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3, linestyle="--")
    ax.legend(loc="best", fontsize=11, framealpha=0.9)
    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter("%Y-%m"))
    ax.xaxis.set_major_locator(plt.matplotlib.dates.YearLocator())
    plt.xticks(rotation=45)
    fig.tight_layout()
    return fig
