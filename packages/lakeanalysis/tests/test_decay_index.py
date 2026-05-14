"""Tests for the corrected log-sum-exp decay index."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd

from lakeanalysis.pwm_extreme.events import compute_decay_index, extract_segments


def _labels_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4],
            "water_area": [100.0, 110.0, 90.0, 100.0],
            "index_value": [95.0, 50.0, 3.0, 50.0],
            "threshold_low": [5.0, 5.0, 5.0, 5.0],
            "threshold_high": [90.0, 90.0, 90.0, 90.0],
            "extreme_label": ["extreme_high", "normal", "extreme_low", "normal"],
        }
    )


def test_compute_decay_index_uses_exceedance_fallback() -> None:
    result = compute_decay_index(_labels_df(), decay_rate=1.0)

    assert np.isclose(result.loc[0, "phi_i"], 5.0)
    assert np.isclose(result.loc[2, "phi_i"], 2.0)
    assert np.isclose(result.loc[1, "phi_i"], 0.0)


def test_compute_decay_index_matches_log_sum_exp_definition() -> None:
    result = compute_decay_index(_labels_df(), decay_rate=1.0)

    expected = [
        math.log(5.0),
        math.log(5.0 * math.exp(-1.0)),
        math.log(5.0 * math.exp(-2.0) + 2.0),
        math.log(5.0 * math.exp(-3.0) + 2.0 * math.exp(-1.0)),
    ]
    assert np.allclose(result["C_k"].to_numpy(dtype=float), np.array(expected))


def test_compute_decay_index_uses_custom_phi_df() -> None:
    phi_df = pd.DataFrame(
        {
            "year": [2000, 2000],
            "month": [1, 3],
            "phi": [2.0, 10.0],
        }
    )

    result = compute_decay_index(_labels_df(), decay_rate=0.5, phi_df=phi_df)
    assert np.isclose(result.loc[0, "phi_i"], 2.0)
    assert np.isclose(result.loc[2, "phi_i"], 10.0)


def test_extract_segments_uses_positive_ck_active_rule() -> None:
    result = compute_decay_index(_labels_df(), decay_rate=1.0)
    segments = extract_segments(result)

    assert len(segments) == 1
    seg = segments.iloc[0]
    assert seg["segment_type"] == "transition"
    assert int(seg["duration_months"]) == 3
