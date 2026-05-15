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
    # Add threshold columns BEFORE calling assign_extreme_labels
    df["threshold_low"] = 0.5
    df["threshold_high"] = 1.5
    return assign_extreme_labels(df, 0.5, 1.5)


# ── compute_decay_index ────────────────────────────────────────────────────

class TestComputeDecayIndex:
    def test_normal_months_dont_inject_phi(self):
        """Normal months have phi_i=0 and don't increase S_k."""
        labeled_df = _make_labeled_df(
            [2000] * 4, [1, 2, 3, 4],
            [1.0, 1.1, 1.0, 1.2],  # all normal (within [0.5, 1.5])
            [10.0, 11.0, 10.0, 12.0],
        )
        result = compute_decay_index(labeled_df)
        assert (result["phi_i"] == 0).all()
        assert (result["has_high"] == False).all()  # noqa: E712
        assert (result["has_low"] == False).all()  # noqa: E712

    def test_extreme_month_injects_exceedance_as_phi(self):
        """An extreme-low month injects phi = threshold_low - index_value."""
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 1.0, 1.1],  # month 1 = extreme_low (0.3 < 0.5)
            [3.0, 10.0, 11.0],
        )
        result = compute_decay_index(labeled_df)
        # Month 1: phi = 0.5 - 0.3 = 0.2, S_k = 0.2
        assert result.iloc[0]["phi_i"] == pytest.approx(0.2)
        assert result.iloc[0]["S_k"] == pytest.approx(0.2)
        # Month 2: normal → no phi, S_k decays: 0.2 * e^(-1) ≈ 0.0736
        assert result.iloc[1]["phi_i"] == 0.0
        assert 0 < result.iloc[1]["S_k"] < result.iloc[0]["S_k"]

    def test_high_extreme_injects_threshold_diff(self):
        """An extreme-high month injects phi = index_value - threshold_high."""
        labeled_df = _make_labeled_df(
            [2000, 2000],
            [1, 2],
            [2.0, 1.0],  # month 1 = extreme_high (2.0 > 1.5)
            [20.0, 10.0],
        )
        result = compute_decay_index(labeled_df)
        assert result.iloc[0]["phi_i"] == pytest.approx(0.5)  # 2.0 - 1.5

    def test_s_k_decays_exponentially_over_normal_gap(self):
        """S_k decays by e^{-gap*decay_rate} across normal months."""
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 1.0, 1.1],  # extreme-low at month 1 only
            [3.0, 10.0, 11.0],
        )
        result = compute_decay_index(labeled_df, decay_rate=0.5)
        # Month 1: phi = 0.2, S_1 = 0.2
        # Month 2: gap=1, decay = 0.2 * e^{-0.5} + 0 = 0.1213
        # Month 3: gap=1, decay = 0.1213 * e^{-0.5} + 0 = 0.0736
        assert result.iloc[1]["S_k"] == pytest.approx(0.2 * np.exp(-0.5), rel=1e-4)
        assert result.iloc[2]["S_k"] == pytest.approx(0.2 * np.exp(-1.0), rel=1e-4)

    def test_missing_required_column_raises(self):
        df = pd.DataFrame({"year": [2000], "month": [1], "index_value": [1.0]})
        with pytest.raises(ValueError, match="missing required columns"):
            compute_decay_index(df)

    def test_has_high_and_has_low_flags_correct(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 1.0, 2.0],  # low, normal, high
            [3.0, 10.0, 20.0],
        )
        result = compute_decay_index(labeled_df)
        assert result.iloc[0]["has_low"] == True  # noqa: E712
        assert result.iloc[2]["has_high"] == True  # noqa: E712
        assert result.iloc[1]["has_high"] == False  # noqa: E712
        assert result.iloc[1]["has_low"] == False  # noqa: E712


# ── extract_segments ───────────────────────────────────────────────────────

class TestExtractSegments:
    def test_transition_segment_has_both_types(self):
        """Adjacent low→high extremes produce a transition segment."""
        labeled_df = _make_labeled_df(
            [2000, 2000], [1, 2],
            [0.3, 2.0],   # low then high — adjacent extremes
            [3.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        assert len(segments_df) == 1
        seg = segments_df.iloc[0]
        assert seg["segment_type"] == "transition"
        assert seg["has_high"] == True  # noqa: E712
        assert seg["has_low"] == True  # noqa: E712
        assert seg["first_extreme_type"] == "low"
        assert seg["last_extreme_type"] == "high"

    def test_two_consecutive_normals_break_segment(self):
        """Two consecutive normal months always break into separate segments."""
        labeled_df = _make_labeled_df(
            [2000] * 6, [1, 2, 3, 4, 5, 6],
            [0.3, 1.0, 1.0, 0.3, 1.0, 2.0],
            [3.0, 10.0, 10.0, 3.0, 10.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        # months 2-3 are consecutive normals → break. Result: months 1 (secluded),
        # month 4 (secluded), month 6 (secluded). No segments span the normals.
        assert len(segments_df) >= 2
        # Each segment contains only months of its own type cluster
        segment_months = []
        for _, seg in segments_df.iterrows():
            segment_months.append((seg["start_month"], seg["end_month"]))
        # verify no segment spans across months 2-3 break
        for start, end in segment_months:
            assert not (start <= 2 and end >= 3)

    def test_all_normal_produces_no_segments(self):
        labeled_df = _make_labeled_df(
            [2000] * 3, [1, 2, 3],
            [1.0, 1.0, 1.0],
            [10.0, 10.0, 10.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        assert segments_df.empty

    def test_unilateral_segment_has_only_one_type(self):
        """Unilateral segment has exactly one of has_high/has_low."""
        labeled_df = _make_labeled_df(
            [2000] * 4, [1, 2, 3, 4],
            [0.3, 0.2, 1.0, 1.1],  # two lows, then normal, normal
            [3.0, 2.0, 10.0, 11.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        seg = segments_df.iloc[0]
        assert seg["segment_type"] == "unilateral"
        assert seg["has_high"] != seg["has_low"]

    def test_segment_duration_equal_or_greater_than_two(self):
        """A valid segment must have at least 2 months (2 extreme months)."""
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [0.3, 0.2, 2.0, 2.1],  # adjacent extremes
            [3.0, 2.0, 20.0, 21.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        assert (segments_df["duration_months"] >= 2).all()

    def test_first_and_last_extreme_type_recorded(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 2.0],  # low, low, high — adjacent
            [3.0, 2.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        seg = segments_df.iloc[0]
        assert seg["first_extreme_type"] == "low"
        assert seg["last_extreme_type"] == "high"


# ── extract_hawkes_events_from_segments ────────────────────────────────────

class TestExtractHawkesEventsFromSegments:
    def test_emits_both_high_and_low_events(self):
        """Adjacent low→high extremes produce both event types."""
        labeled_df = _make_labeled_df(
            [2000, 2000], [1, 2],
            [0.3, 2.0],
            [3.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        assert set(events["event_type"].unique()) == {"high", "low"}

    def test_severity_equals_abs_index_minus_threshold(self):
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 2.0],  # adjacent extremes
            [3.0, 2.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        for _, row in events.iterrows():
            expected = abs(row["index_value"] - row["threshold"])
            assert row["severity"] == pytest.approx(expected, rel=1e-6)

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

    def test_consecutive_same_type_collapsed_to_max_severity(self):
        """Two consecutive lows in same segment collapse to one event (max severity)."""
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 0.2, 2.0],  # low, low, high — adjacent extremes
            [3.0, 2.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        # Months 1-2 are consecutive lows → collapsed to one (month 2, severity=0.3)
        low_events = events[events["event_type"] == "low"]
        assert len(low_events) == 1
        assert low_events.iloc[0]["severity"] == pytest.approx(0.3)

    def test_different_type_events_not_collapsed(self):
        """A low then a high in the same segment are NOT collapsed."""
        labeled_df = _make_labeled_df(
            [2000, 2000],
            [1, 2],
            [0.3, 2.0],  # low, high — adjacent
            [3.0, 20.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        assert len(events) == 2
        assert set(events["event_type"].unique()) == {"high", "low"}

    def test_time_is_strictly_increasing(self):
        """Event timestamps are monotonic."""
        labeled_df = _make_labeled_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [0.3, 2.0, 0.2],  # low, high, low — adjacent
            [3.0, 20.0, 2.0],
        )
        decay_df = compute_decay_index(labeled_df)
        segments_df = extract_segments(decay_df)
        events = extract_hawkes_events_from_segments(labeled_df, decay_df, segments_df)
        times = events["time"].tolist()
        for i in range(1, len(times)):
            assert times[i] > times[i - 1]


# ── run_runs_declustering ──────────────────────────────────────────────────

class TestRunsDeclustering:
    def test_single_event_has_cluster_size_one(self):
        df = pd.DataFrame({
            "year": [2000], "month": [1], "event_type": ["high"],
            "index_value": [1.0], "threshold": [0.5], "water_area": [10.0],
            "severity": [0.5], "time": [2000.0],
        })
        result = run_runs_declustering(df)
        assert len(result) == 1
        assert result.iloc[0]["cluster_size"] == 1

    def test_gapped_events_not_merged(self):
        df = pd.DataFrame({
            "year": [2000, 2000], "month": [1, 3], "event_type": ["high", "high"],
            "index_value": [1.0, 1.2], "threshold": [0.5, 0.5], "water_area": [10.0, 12.0],
            "severity": [0.5, 0.7], "time": [2000.0, 2000.1667],
        })
        result = run_runs_declustering(df)
        assert len(result) == 2

    def test_same_type_consecutive_merged_to_max_severity(self):
        df = pd.DataFrame({
            "year": [2000, 2000], "month": [1, 2], "event_type": ["high", "high"],
            "index_value": [1.0, 1.5], "threshold": [0.5, 0.5], "water_area": [10.0, 15.0],
            "severity": [0.5, 1.0], "time": [2000.0, 2000.0833],
        })
        result = run_runs_declustering(df)
        assert len(result) == 1
        assert result.iloc[0]["cluster_size"] == 2
        assert result.iloc[0]["severity"] == 1.0

    def test_different_type_breaks_cluster(self):
        df = pd.DataFrame({
            "year": [2000, 2000], "month": [1, 2], "event_type": ["high", "low"],
            "index_value": [1.0, 0.3], "threshold": [0.5, 0.5], "water_area": [10.0, 3.0],
            "severity": [0.5, 0.2], "time": [2000.0, 2000.0833],
        })
        result = run_runs_declustering(df)
        assert len(result) == 2
