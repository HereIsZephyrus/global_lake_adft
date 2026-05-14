"""Tests for PWM-extreme events and declustering."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.pwm.events import (
    build_hawkes_event_series_from_pwm_events,
    run_runs_declustering,
)
from lakeanalysis.hawkes.types import TYPE_DRY, TYPE_WET


@pytest.fixture()
def simple_events_df():
    return pd.DataFrame(
        {
            "year": [2000, 2000, 2001, 2002, 2002, 2003],
            "month": [1, 2, 3, 5, 6, 1],
            "event_type": ["high", "high", "low", "high", "high", "low"],
            "water_area": [180.0, 190.0, 30.0, 200.0, 150.0, 25.0],
            "threshold": [150.0, 150.0, 40.0, 150.0, 150.0, 40.0],
            "severity": [30.0, 40.0, 10.0, 50.0, 0.0, 15.0],
        }
    )


@pytest.fixture()
def series_df():
    records = []
    for year in range(2000, 2004):
        for month in range(1, 13):
            records.append(
                {
                    "year": year,
                    "month": month,
                    "water_area": 100.0 + 20.0 * np.sin(2 * np.pi * month / 12),
                }
            )
    return pd.DataFrame(records)


class TestRunRunsDeclustering:
    def test_empty_returns_empty(self):
        df = pd.DataFrame()
        result = run_runs_declustering(df)
        assert result.empty
        assert "cluster_id" in result.columns

    def test_single_cluster_same_type_consecutive(self, simple_events_df):
        result = run_runs_declustering(simple_events_df, run_length=1)
        high_clusters = result[result["event_type"] == "high"]
        low_clusters = result[result["event_type"] == "low"]
        assert len(high_clusters) >= 1
        assert len(low_clusters) >= 1
        first_high = high_clusters[high_clusters["year"] == 2000]
        assert len(first_high) == 1
        assert first_high.iloc[0]["severity"] == 40.0

    def test_different_type_breaks_cluster(self):
        df = pd.DataFrame(
            {
                "year": [2000, 2000],
                "month": [1, 2],
                "event_type": ["high", "low"],
                "water_area": [180.0, 30.0],
                "threshold": [150.0, 40.0],
                "severity": [30.0, 10.0],
            }
        )
        result = run_runs_declustering(df, run_length=3)
        assert len(result) == 2

    def test_representative_is_max_severity(self):
        df = pd.DataFrame(
            {
                "year": [2000, 2000],
                "month": [1, 2],
                "event_type": ["high", "high"],
                "water_area": [180.0, 200.0],
                "threshold": [150.0, 150.0],
                "severity": [30.0, 50.0],
            }
        )
        result = run_runs_declustering(df, run_length=1)
        assert len(result) == 1
        assert result.iloc[0]["severity"] == 50.0


class TestBuildHawkesEventSeriesFromPWM:
    def test_empty_events(self, series_df):
        empty_df = pd.DataFrame()
        event_series, events_table = build_hawkes_event_series_from_pwm_events(
            empty_df, series_df
        )
        assert len(event_series.times) == 0
        assert events_table.empty

    def test_high_maps_to_wet(self, series_df):
        events = pd.DataFrame(
            {
                "year": [2000],
                "month": [1],
                "event_type": ["high"],
                "water_area": [180.0],
                "threshold": [150.0],
                "severity": [30.0],
                "time": [2000.0],
                "cluster_id": [1],
                "cluster_size": [1],
            }
        )
        event_series, events_table = build_hawkes_event_series_from_pwm_events(
            events, series_df
        )
        assert len(event_series.times) == 1
        assert int(event_series.event_types[0]) == TYPE_WET

    def test_low_maps_to_dry(self, series_df):
        events = pd.DataFrame(
            {
                "year": [2000],
                "month": [1],
                "event_type": ["low"],
                "water_area": [30.0],
                "threshold": [40.0],
                "severity": [10.0],
                "time": [2000.0],
                "cluster_id": [1],
                "cluster_size": [1],
            }
        )
        event_series, events_table = build_hawkes_event_series_from_pwm_events(
            events, series_df
        )
        assert len(event_series.times) == 1
        assert int(event_series.event_types[0]) == TYPE_DRY

    def test_event_series_valid(self, series_df, simple_events_df):
        declustered = run_runs_declustering(simple_events_df, run_length=1)
        event_series, _ = build_hawkes_event_series_from_pwm_events(
            declustered, series_df
        )
        assert event_series.start_time >= 2000.0
        assert event_series.end_time > event_series.start_time
        assert len(event_series.times) > 0
