"""Bridge utilities from EOT event extraction to Hawkes event sequences."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.eot import EOTEstimator, NoDeclustering

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
) -> HawkesEventSeries:
    """Build a dual-type event series from EOT outputs with NoDeclustering."""
    estimator = EOTEstimator(declustering_strategy=NoDeclustering())
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

