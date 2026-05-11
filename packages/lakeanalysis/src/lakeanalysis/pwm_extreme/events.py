"""PWM extreme event extraction with runs declustering for Hawkes input."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.hawkes.types import (
    HawkesEventSeries,
    TYPE_DRY,
    TYPE_LABELS,
    TYPE_WET,
)


def run_runs_declustering(
    events_df: pd.DataFrame,
    *,
    run_length: int = 1,
) -> pd.DataFrame:
    """Apply runs declustering to PWM extreme events.

    Consecutive events of the same type are merged into one cluster when
    the gap in months between them is at most ``run_length``.  The
    representative event is the one with the largest severity within the
    cluster.

    Args:
        events_df: Output from ``extract_pwm_extreme_events``, sorted by
            year/month.  Must contain columns: year, month, event_type,
            water_area, threshold, severity.
        run_length: Minimum number of normal months required between two
            events of the same type for them to belong to separate
            clusters.

    Returns:
        DataFrame with the same columns as input, plus ``cluster_id`` and
        ``cluster_size``, containing one representative row per cluster.
    """
    if events_df.empty:
        return pd.DataFrame(
            columns=list(events_df.columns) + ["cluster_id", "cluster_size", "time"]
        )

    df = events_df.sort_values(["year", "month"]).reset_index(drop=True)
    df["time"] = df["year"] + (df["month"] - 1) / 12.0
    df["month_seq"] = df["year"] * 12 + df["month"]

    clusters_records: list[list[dict]] = []
    current_cluster: list[dict] = []

    for _, row in df.iterrows():
        if not current_cluster:
            current_cluster.append(row.to_dict())
        elif (
            row["event_type"] == current_cluster[-1]["event_type"]
            and row["month_seq"] - current_cluster[-1]["month_seq"] - 1 < run_length
        ):
            current_cluster.append(row.to_dict())
        else:
            clusters_records.append(current_cluster)
            current_cluster = [row.to_dict()]

    if current_cluster:
        clusters_records.append(current_cluster)

    records: list[dict] = []
    for cluster_id, cluster in enumerate(clusters_records, start=1):
        representative = max(cluster, key=lambda r: r.get("severity", 0.0))
        records.append(
            {
                "year": int(representative["year"]),
                "month": int(representative["month"]),
                "event_type": representative["event_type"],
                "water_area": float(representative["water_area"]),
                "threshold": float(representative["threshold"]),
                "severity": float(representative["severity"]),
                "cluster_id": cluster_id,
                "cluster_size": len(cluster),
                "time": float(representative["time"]),
            }
        )

    if not records:
        return pd.DataFrame(
            columns=list(events_df.columns) + ["cluster_id", "cluster_size", "time"]
        )

    return pd.DataFrame(records)


def build_hawkes_event_series_from_pwm_events(
    events_df: pd.DataFrame,
    series_df: pd.DataFrame,
) -> tuple[HawkesEventSeries, pd.DataFrame]:
    """Construct a dual-type HawkesEventSeries from PWM declustered events.

    ``extreme_high`` events are mapped to wet (TYPE_WET), ``extreme_low``
    events are mapped to dry (TYPE_DRY).

    Args:
        events_df: Declustered PWM events with columns year, month,
            event_type, water_area, threshold, severity, time,
            cluster_id, cluster_size.
        series_df: Original monthly series with columns year, month,
            water_area.

    Returns:
        (HawkesEventSeries, events_table) where events_table includes
        Hawkes-style event_label and event_type columns.
    """
    if events_df.empty:
        timeline = _build_timeline(series_df)
        empty_table = pd.DataFrame(
            columns=[
                "time",
                "year",
                "month",
                "event_type",
                "event_label",
                "water_area",
                "threshold",
                "severity",
                "cluster_id",
                "cluster_size",
            ]
        )
        return HawkesEventSeries(
            times=np.array([], dtype=float),
            event_types=np.array([], dtype=int),
            start_time=float(timeline["time"].min()),
            end_time=float(timeline["time"].max() + 1.0 / 12.0),
            timeline=timeline,
            events_table=empty_table,
        ), empty_table

    events = events_df.copy()

    high_mask = events["event_type"] == "high"
    low_mask = events["event_type"] == "low"

    events.loc[high_mask, "event_label"] = TYPE_LABELS[TYPE_WET]
    events.loc[high_mask, "event_type_code"] = TYPE_WET
    events.loc[low_mask, "event_label"] = TYPE_LABELS[TYPE_DRY]
    events.loc[low_mask, "event_type_code"] = TYPE_DRY

    events["event_type_code"] = events["event_type_code"].astype(int)

    events_table = events.loc[
        :,
        [
            "time",
            "year",
            "month",
            "event_type_code",
            "event_label",
            "water_area",
            "threshold",
            "severity",
            "cluster_id",
            "cluster_size",
        ],
    ].rename(columns={"event_type_code": "event_type"})

    events_table = events_table.sort_values(
        ["time", "event_type"], ascending=[True, True]
    ).reset_index(drop=True)

    timeline = _build_timeline(series_df)
    start_time = float(timeline["time"].min())
    end_time = float(timeline["time"].max() + 1.0 / 12.0)

    event_series = HawkesEventSeries(
        times=events_table["time"].to_numpy(dtype=float),
        event_types=events_table["event_type"].to_numpy(dtype=int),
        start_time=start_time,
        end_time=end_time,
        timeline=timeline,
        events_table=events_table,
    )
    return event_series, events_table


def _build_timeline(series_df: pd.DataFrame) -> pd.DataFrame:
    """Build a timeline DataFrame from the monthly series."""
    tl = series_df.loc[:, ["year", "month"]].drop_duplicates().copy()
    tl["year"] = tl["year"].astype(int)
    tl["month"] = tl["month"].astype(int)
    tl["time"] = tl["year"] + (tl["month"] - 1) / 12.0
    return tl.sort_values("time").reset_index(drop=True)
