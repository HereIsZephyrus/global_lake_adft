"""Tests for event-strength to phi mapping."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.pwm_extreme.phi import map_strength_df_to_phi, map_strength_to_phi


def test_map_strength_to_phi_identity() -> None:
    values = map_strength_to_phi(np.array([0.0, 1.0, 2.5]), method="identity")
    assert np.allclose(values, np.array([0.0, 1.0, 2.5]))


def test_map_strength_to_phi_log1p() -> None:
    values = map_strength_to_phi(np.array([0.0, 1.0]), method="log1p")
    assert np.allclose(values, np.log1p(np.array([0.0, 1.0])))


def test_map_strength_to_phi_normalize_uses_mean_positive_reference() -> None:
    values = map_strength_to_phi(np.array([0.0, 2.0, 4.0]), method="normalize")
    assert np.allclose(values, np.array([0.0, 2.0 / 3.0, 4.0 / 3.0]))


def test_map_strength_df_to_phi_adds_phi_column() -> None:
    df = pd.DataFrame(
        {
            "year": [2000],
            "month": [1],
            "event_strength": [5.0],
        }
    )
    result = map_strength_df_to_phi(df)
    assert list(result.columns) == ["year", "month", "event_strength", "phi"]
    assert float(result.iloc[0]["phi"]) == 5.0


def test_map_strength_to_phi_rejects_negative_strength() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        map_strength_to_phi(np.array([-1.0]), method="identity")
