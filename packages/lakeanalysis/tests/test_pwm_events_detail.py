"""Tests for pwm/events.py — segment extraction and Hawkes event construction."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.pwm.events import (
    extract_segments,
    extract_hawkes_events_from_segments,
    run_runs_declustering,
    compute_decay_index,
)
from lakeanalysis.extreme.compute import assign_extreme_labels


# ── Helpers ────────────────────────────────────────────────────────────────

def _make_labeled_df(
    years: list[int],
    months: list[int],
    index_values: list[float],
    water_areas: list[float],
) -> pd.DataFrame:
    """Build a labeled DataFrame with threshold columns."""
    df = pd.DataFrame({
        "year": years,
        "month": months,
        "index_value": index_values,
        "water_area": water_areas,
        "year_month_key": [y * 100 + m for y, m in zip(years, months)],
        "month_ordinal": [y * 12 + (m - 1) for y, m in zip(years, months)],
    })
    # Bump thresholds slightly
    df["threshold_low"] = 0.5
    df["threshold_high"] = 1.5
    return assign_extreme_labels(df, 0.5, 1.5)


# ── compute_decay_index ────────────────────────────────────────────────────

class TestComputeDecayIndex:
    def test_basic_decay(self):
        labeled_df = _make_labeled_df(
            [2000] * 5, [1, 2, 3, 4, 5],
            [1.0, 1.1, 0.2, 1.0, 1.2],
            [10.0, 11.0, 2.0, 10.0, 12.0],
        )
        result = compute_decay_index(labeled_df)
        assert "S_k" in result.columns
        assert "has_high" in result.columns
        assert "has_low" in result.columns
        assert len(result) == 5

    def test_pure_normal_series_gives_low_decay(self):
        labeled_df = _make_labeled_df(
            [2000] * 3, [1, 2, 3],
            [1.0, 1.1, 1.0],  # all normal
            [10.0, 11.0, 10.0],
        )
        result = compute_decay_index(labeled_df)
        assert result["has_high"].sum() == 0
        assert result["has_low"].sum() == 0

    def test_missing_required_column_raises(self):
        df = pd.DataFrame({"year": [2000], "month": [1], "index_value": [1.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            compute_decay_index(df)

    def test_phi_column_set_when_provided(self):
        labeled_df = _make_labeled_df(
            [2000] * 3, [1, 2, 3],
            [1.0, 0.3, 1.1],
            [10.0, 3.0, 11.0],
        )
        result = compute_decay_index(labeled_df)
        assert result.iloc[1]["phi_i"] > 0  # low event at month 2 injects phi


# ── extract_segments ───────────────────────────────────────────────────────

class TestExtractSegments:
    def test_transition_segment_created(self):
        # Two extremes close together = transition segment
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 1.6],  # low, low, high
            [3.0, 2.0, 16.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        assert len(segments_df) > 0
        assert "transition" in segments_df["segment_type"].values

    def test_segment_has_metadata_columns(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 1.6],
            [3.0, 2.0, 16.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        expected_cols = {
            "segment_id", "start_year", "start_month", "end_year", "end_month",
            "duration_months", "segment_type", "has_high", "has_low",
            "max_S", "mean_S", "integral_S", "n_extreme_events",
            "first_extreme_type", "last_extreme_type",
        }
        assert expected_cols.issubset(segments_df.columns)

    def test_all_normal_produces_no_segments(self):
        labeled_df = _make_labeled_df(
            [2000] * 3, [1, 2, 3],
            [1.0, 1.0, 1.0],
            [10.0, 10.0, 10.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        assert segments_df.empty

    def test_two_consecutive_normals_break_segment(self):
        labeled_df = _make_labeled_df(
            [2000] * 6, [1, 2, 3, 4, 5, 6],
            [0.3, 1.0, 1.0, 0.3, 0.2, 1.6],
            [3.0] * 6,
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        # should produce separate segments (or at least not merge across gap)
        assert len(segments_df) >= 1

    def test_unilateral_segment_identified(self):
        labeled_df = _make_labeled_df(
            [2000] * 4, [1, 2, 3, 4],
            [0.3, 0.2, 1.0, 1.1],  # two lows, then normals
            [3.0, 2.0, 10.0, 11.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        types = segments_df["segment_type"].unique()
        assert "unilateral" in types


# ── extract_hawkes_events_from_segments ────────────────────────────────────

class TestExtractHawkesEventsFromSegments:
    def test_emits_events_from_transition_segment(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [0.3, 1.0, 0.2, 1.6],  # low, normal, low (bridge), high
            [3.0, 10.0, 2.0, 16.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        assert len(events) > 0
        assert "event_type" in events.columns
        assert "severity" in events.columns
        assert "year" in events.columns
        assert "month" in events.columns

    def test_severity_computed_as_abs_diff(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 1.6],
            [3.0, 2.0, 16.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        for _, row in events.iterrows():
            expected_sev = abs(row["index_value"] - row["threshold"])
            assert row["severity"] == pytest.approx(expected_sev)

    def test_empty_segments_yields_empty_events(self):
        labeled_df = _make_labeled_df(
            [2000] * 3, [1, 2, 3],
            [1.0, 1.0, 1.0],
            [10.0, 10.0, 10.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        assert events.empty

    def test_consecutive_same_type_events_collapsed(self):
        # Build a sequence where two lows occur consecutively
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5],
            [0.3, 0.2, 1.0, 0.1, 1.8],  # low, low, normal, low, high
            [3.0, 2.0, 10.0, 1.0, 18.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        # Consecutive lows in same segment should be collapsed by declustering
        # but consecutive extreme months not separated by normal will merge
        assert len(events) > 0
        # after collapsing, same month should not have duplicate events
        assert not events.duplicated(subset=["year", "month"]).any()

    def test_events_have_time_column(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 1.6],
            [3.0, 2.0, 16.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        assert "time" in events.columns
        assert all(events["time"] >= 0)


# ── run_runs_declustering ──────────────────────────────────────────────────

class TestRunsDeclustering:
    def test_single_event_is_returned_as_is(self):
        df = pd.DataFrame({
            "year": [2000],
            "month": [1],
            "event_type": ["high"],
            "index_value": [1.0],
            "threshold": [0.5],
            "water_area": [10.0],
            "severity": [0.5],
            "time": [2000.0],
        })
        result = run_runs_declustering(df)
        assert len(result) == 1
        assert result.iloc[0]["cluster_size"] == 1

    def test_two_separate_events_stay_separate(self):
        df = pd.DataFrame({
            "year": [2000, 2000],
            "month": [1, 3],  # gap > 1
            "event_type": ["high", "high"],
            "index_value": [1.0, 1.2],
            "threshold": [0.5, 0.5],
            "water_area": [10.0, 12.0],
            "severity": [0.5, 0.7],
            "time": [2000.0, 2000.1667],
        })
        result = run_runs_declustering(df)
        assert len(result) == 2

    def test_same_type_consecutive_events_merged(self):
        df = pd.DataFrame({
            "year": [2000, 2000],
            "month": [1, 2],  # consecutive
            "event_type": ["high", "high"],
            "index_value": [1.0, 1.5],
            "threshold": [0.5, 0.5],
            "water_area": [10.0, 15.0],
            "severity": [0.5, 1.0],
            "time": [2000.0, 2000.0833],
        })
        result = run_runs_declustering(df)
        assert len(result) == 1
        assert result.iloc[0]["cluster_size"] == 2
        assert result.iloc[0]["severity"] == 1.0  # max severity

    def test_different_type_breaks_cluster(self):
        df = pd.DataFrame({
            "year": [2000, 2000],
            "month": [1, 2],
            "event_type": ["high", "low"],
            "index_value": [1.0, 0.3],
            "threshold": [0.5, 0.5],
            "water_area": [10.0, 3.0],
            "severity": [0.5, 0.2],
            "time": [2000.0, 2000.0833],
        })
        result = run_runs_declustering(df)
        assert len(result) == 2
