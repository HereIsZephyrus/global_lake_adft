"""Tests for Route B amplitude-space EVT helpers."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.pwm.evt_amplitude import compute_evt_amplitude_strengths


def test_compute_evt_amplitude_strengths_uses_residual_exceedance() -> None:
    labeled_df = pd.DataFrame(
        {
            "year": [2000, 2001, 2002, 2000, 2001, 2002],
            "month": [1, 1, 1, 2, 2, 2],
            "extreme_label": [
                "extreme_high",
                "extreme_high",
                "extreme_high",
                "extreme_low",
                "extreme_low",
                "extreme_low",
            ],
            "threshold_low": [5.0] * 6,
            "threshold_high": [90.0] * 6,
            "stl_residual": [4.0, 5.0, 7.0, -6.0, -5.0, -4.0],
        }
    )

    strengths_df, summary_df = compute_evt_amplitude_strengths(labeled_df)

    assert len(strengths_df) == 6
    assert (strengths_df["event_strength"] >= 0.0).all()
    assert (strengths_df["threshold"].notna()).all()
    assert strengths_df.loc[strengths_df["tail"] == "high", "threshold"].nunique() == 1
    assert strengths_df.loc[strengths_df["tail"] == "low", "threshold"].nunique() == 1
    assert float(strengths_df.loc[0, "threshold"]) > 4.0
    assert float(strengths_df.loc[3, "threshold"]) < -4.0
    assert set(summary_df["evt_route"]) == {"B"}
