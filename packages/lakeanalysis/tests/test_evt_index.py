"""Tests for Route A EVT helpers on index_value exceedances."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.pwm_extreme.evt_index import compute_evt_index_strengths


def test_compute_evt_index_strengths_uses_exceedance_as_strength() -> None:
    labeled_df = pd.DataFrame(
        {
            "year": [2000, 2000, 2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4, 5, 6],
            "index_value": [97.0, 96.0, 95.0, 4.0, 3.0, 2.0],
            "threshold_low": [5.0] * 6,
            "threshold_high": [90.0] * 6,
            "extreme_label": [
                "extreme_high",
                "extreme_high",
                "extreme_high",
                "extreme_low",
                "extreme_low",
                "extreme_low",
            ],
        }
    )

    strengths_df, summary_df = compute_evt_index_strengths(labeled_df)

    assert list(strengths_df["event_strength"]) == [7.0, 6.0, 5.0, 1.0, 2.0, 3.0]
    assert set(summary_df["tail"]) == {"high", "low"}
    assert bool(summary_df["converged"].all())
    assert set(summary_df["evt_route"]) == {"A"}


def test_compute_evt_index_strengths_falls_back_when_fit_fails() -> None:
    labeled_df = pd.DataFrame(
        {
            "year": [2000, 2000],
            "month": [1, 2],
            "index_value": [97.0, 4.0],
            "threshold_low": [5.0, 5.0],
            "threshold_high": [90.0, 90.0],
            "extreme_label": ["extreme_high", "extreme_low"],
        }
    )

    strengths_df, summary_df = compute_evt_index_strengths(labeled_df)

    assert list(strengths_df["event_strength"]) == [7.0, 1.0]
    assert not bool(summary_df["converged"].all())
