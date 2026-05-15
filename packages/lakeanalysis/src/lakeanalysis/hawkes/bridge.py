"""Bridge utilities from EOT and PWM event extraction to Hawkes event sequences."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.eot import EOTEstimator, RunsDeclustering

from .types import HawkesEventSeries, TYPE_DRY, TYPE_LABELS, TYPE_WET


def _events_with_label(extremes: pd.DataFrame, event_label: str) -> pd.DataFrame:
    """Return standardized event rows for one event type."""
    if extremes.empty:
        return pd.DataFrame(
            columns=[
                "time",
                "year",
                "month",
                "event_type",
                "event_label",
                "value",
                "original_value",
                "threshold",
            ]
        )
    out = extremes.loc[
        :,
        ["time", "year", "month", "value", "original_value", "threshold"],
    ].copy()
    out["event_label"] = event_label
    out["event_type"] = TYPE_DRY if event_label == TYPE_LABELS[TYPE_DRY] else TYPE_WET
    return out


def build_events_from_eot(
    data: pd.DataFrame,
    threshold_quantile: float = 0.90,
    frozen_year_months: set[int] | None = None,
    declustering_strategy: object | None = None,
) -> HawkesEventSeries:
    """Build a dual-type event series from EOT outputs.

    Defaults to RunsDeclustering(run_length=1) to decluster consecutive
    exceedances.  Pass NoDeclustering() for comparison / baseline runs.

    Args:
        data: Monthly water area DataFrame.
        threshold_quantile: EOT threshold quantile.
        frozen_year_months: Optional set of frozen year-month integers.
        declustering_strategy: DeclusteringStrategy instance (default
            RunsDeclustering(run_length=1)).

    Returns:
        HawkesEventSeries ready for fitting.
    """
    if declustering_strategy is None:
        declustering_strategy = RunsDeclustering(run_length=1)
    estimator = EOTEstimator(declustering_strategy=declustering_strategy)
    prepared_wet = estimator.prepare_extremes(
        data=data,
        tail="high",
        threshold=None,
        threshold_quantile=threshold_quantile,
        frozen_year_months=frozen_year_months,
    )
    prepared_dry = estimator.prepare_extremes(
        data=data,
        tail="low",
        threshold=None,
        threshold_quantile=threshold_quantile,
        frozen_year_months=frozen_year_months,
    )

    wet_events = _events_with_label(prepared_wet.extremes, TYPE_LABELS[TYPE_WET])
    dry_events = _events_with_label(prepared_dry.extremes, TYPE_LABELS[TYPE_DRY])
    events_table = pd.concat([dry_events, wet_events], ignore_index=True)
    if not events_table.empty:
        events_table = events_table.sort_values(
            ["time", "event_type"],
            ascending=[True, True],
        ).reset_index(drop=True)
    timeline = prepared_wet.series.data.loc[:, ["year", "month", "time"]].copy().reset_index(drop=True)
    start_time = float(timeline["time"].min())
    end_time = float(timeline["time"].max() + 1.0 / 12.0)
    return HawkesEventSeries(
        times=events_table["time"].to_numpy(dtype=float),
        event_types=events_table["event_type"].to_numpy(dtype=int),
        start_time=start_time,
        end_time=end_time,
        timeline=timeline,
        events_table=events_table,
    )


def _build_timeline(series_df: pd.DataFrame) -> pd.DataFrame:
    tl = series_df.loc[:, ["year", "month"]].drop_duplicates().copy()
    tl["year"] = tl["year"].astype(int)
    tl["month"] = tl["month"].astype(int)
    tl["time"] = tl["year"] + (tl["month"] - 1) / 12.0
    return tl.sort_values("time").reset_index(drop=True)


def build_events_from_pwm(
    events_df: pd.DataFrame,
    series_df: pd.DataFrame,
) -> tuple[HawkesEventSeries, pd.DataFrame]:
    """Construct a dual-type HawkesEventSeries from PWM events.

    ``extreme_high`` events are mapped to wet (TYPE_WET), ``extreme_low``
    events are mapped to dry (TYPE_DRY).

    Args:
        events_df: PWM events (raw or declustered) with columns year,
            month, event_type, water_area, threshold, severity, time.
            Optional columns: cluster_id, cluster_size, index_value.
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
                "time", "year", "month", "event_type", "event_label",
                "water_area", "threshold", "severity", "index_value",
            ],
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

    table_cols = [
        "time", "year", "month", "event_type_code", "event_label",
        "water_area", "threshold", "severity",
    ]
    for extra in ["index_value", "cluster_id", "cluster_size"]:
        if extra in events.columns:
            table_cols.append(extra)

    events_table = events.loc[:, table_cols].rename(
        columns={"event_type_code": "event_type"}
    )

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
