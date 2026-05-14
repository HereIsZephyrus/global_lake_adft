"""Tests for PWM decay strength and segment extraction."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.pwm.events import compute_decay_index, extract_segments


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


def test_compute_decay_index_matches_linear_strength_definition() -> None:
    result = compute_decay_index(_labels_df(), decay_rate=1.0)

    expected = [
        5.0,
        5.0 * np.exp(-1.0),
        5.0 * np.exp(-2.0) + 2.0,
        5.0 * np.exp(-3.0) + 2.0 * np.exp(-1.0),
    ]
    assert np.allclose(result["S_k"].to_numpy(dtype=float), np.array(expected))


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


def test_compute_decay_index_ignores_phi_on_normal_months() -> None:
    phi_df = pd.DataFrame(
        {
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "phi": [2.0, 99.0, 10.0],
        }
    )

    result = compute_decay_index(_labels_df(), decay_rate=1.0, phi_df=phi_df)

    assert np.isclose(result.loc[0, "phi_i"], 2.0)
    assert np.isclose(result.loc[1, "phi_i"], 0.0)
    assert np.isclose(result.loc[2, "phi_i"], 10.0)


def test_extract_segments_allows_single_bridge_normal_with_positive_strength() -> None:
    result = compute_decay_index(_labels_df(), decay_rate=1.0)
    segments = extract_segments(result)

    assert len(segments) == 1
    seg = segments.iloc[0]
    assert seg["segment_type"] == "transition"
    assert int(seg["duration_months"]) == 3


def test_extract_segments_breaks_when_bridge_strength_drops_below_one() -> None:
    labels_df = pd.DataFrame(
        {
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "water_area": [100.0, 100.0, 100.0],
            "index_value": [95.0, 50.0, 3.0],
            "threshold_low": [5.0, 5.0, 5.0],
            "threshold_high": [90.0, 90.0, 90.0],
            "extreme_label": ["extreme_high", "normal", "extreme_low"],
        }
    )

    result = compute_decay_index(labels_df, decay_rate=2.0)
    assert float(result.loc[1, "S_k"]) < 1.0

    segments = extract_segments(result)
    assert len(segments) == 2
    assert list(segments["duration_months"].astype(int)) == [1, 1]


def test_extract_segments_breaks_on_two_consecutive_normals() -> None:
    decay_df = pd.DataFrame(
        {
            "year": [2000] * 6,
            "month": [1, 2, 3, 4, 5, 6],
            "S_k": [2.0, 1.5, 1.2, 1.1, 1.4, 1.2],
            "has_high": [True, False, False, False, True, False],
            "has_low": [False, False, False, False, False, False],
            "phi_i": [2.0, 0.0, 0.0, 0.0, 1.0, 0.0],
            "exceedance": [2.0, 0.0, 0.0, 0.0, 1.0, 0.0],
        }
    )

    segments = extract_segments(decay_df)
    assert len(segments) == 2
    assert list(segments["duration_months"].astype(int)) == [2, 2]


def test_extract_segments_allows_alternating_extreme_normal_pattern() -> None:
    decay_df = pd.DataFrame(
        {
            "year": [2000] * 5,
            "month": [1, 2, 3, 4, 5],
            "S_k": [1.8, 1.2, 1.6, 1.1, 1.5],
            "has_high": [True, False, False, False, True],
            "has_low": [False, False, True, False, False],
            "phi_i": [1.8, 0.0, 1.0, 0.0, 1.0],
            "exceedance": [1.8, 0.0, 1.0, 0.0, 1.0],
        }
    )

    segments = extract_segments(decay_df)
    assert len(segments) == 1
    seg = segments.iloc[0]
    assert seg["segment_type"] == "transition"
    assert int(seg["duration_months"]) == 5
