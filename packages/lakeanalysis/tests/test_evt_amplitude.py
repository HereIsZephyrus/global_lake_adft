"""Tests for Route B amplitude-space EVT helpers."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.pwm_extreme.evt_amplitude import compute_evt_amplitude_strengths


def test_compute_evt_amplitude_strengths_uses_residual_exceedance() -> None:
    labeled_df = pd.DataFrame(
        {
            "year": [2000, 2000, 2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4, 5, 6],
            "extreme_label": [
                "extreme_high",
                "extreme_high",
                "extreme_high",
                "extreme_low",
                "extreme_low",
                "extreme_low",
            ],
            "stl_residual": [4.0, 5.0, 7.0, -6.0, -5.0, -4.0],
        }
    )

    strengths_df, summary_df = compute_evt_amplitude_strengths(labeled_df)

    assert list(strengths_df["event_strength"]) == [0.0, 1.0, 3.0, 2.0, 1.0, 0.0]
    assert set(summary_df["evt_route"]) == {"B"}
