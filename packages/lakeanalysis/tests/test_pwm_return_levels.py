"""Tests for PWM EVT return-level row shaping."""

from __future__ import annotations

import pandas as pd

from lakesource.pwm_extreme.store import return_levels_to_rows


def test_return_levels_to_rows_converts_summary_frame() -> None:
    summary_df = pd.DataFrame(
        {
            "tail": ["high"],
            "threshold": [90.0],
            "n_total": [100],
            "n_exceedances": [5],
            "shape": [0.1],
            "scale": [2.0],
            "converged": [True],
            "error_message": [None],
            "return_period": [10],
            "return_level": [95.0],
            "evt_route": ["A"],
            "strength_unit": ["index_value"],
        }
    )

    rows = return_levels_to_rows(7, summary_df, workflow_version="test")

    assert len(rows) == 1
    assert rows[0]["hylak_id"] == 7
    assert rows[0]["return_level"] == 95.0
    assert rows[0]["evt_route"] == "A"
