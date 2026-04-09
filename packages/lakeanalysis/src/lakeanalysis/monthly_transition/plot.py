"""Plot helpers for the monthly anomaly transition workflow."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from lakeanalysis.plot_config import setup_chinese_font


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
    monthly_fig.savefig(monthly_path, dpi=300, bbox_inches="tight")
    plt.close(monthly_fig)

    anomaly_fig = plot_anomaly_timeline(labels_df, hylak_id=hylak_id)
    anomaly_fig.savefig(anomaly_path, dpi=300, bbox_inches="tight")
    plt.close(anomaly_fig)

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
    count_fig.savefig(count_path, dpi=300, bbox_inches="tight")
    plt.close(count_fig)

    seasonality_fig = plot_transition_seasonality_summary(transitions_df)
    seasonality_fig.savefig(seasonality_path, dpi=300, bbox_inches="tight")
    plt.close(seasonality_fig)

    return {
        "transition_count_summary": count_path,
        "transition_seasonality": seasonality_path,
    }
