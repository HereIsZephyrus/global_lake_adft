"""Tests for lakeviz grid binning logic."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeviz.grid import build_grid_counts


def _make_sample_df(n: int = 100, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "hylak_id": rng.integers(1, 50, size=n),
        "lat": rng.uniform(-60, 60, size=n),
        "lon": rng.uniform(-180, 180, size=n),
    })


def test_build_grid_counts_returns_geodataframe():
    df = _make_sample_df()
    result = build_grid_counts(df, resolution=0.5)
    assert "geometry" in result.columns
    assert "lake_count" in result.columns
    assert "event_count" in result.columns
    assert "mean_per_lake" in result.columns


def test_build_grid_counts_empty_input():
    df = pd.DataFrame(columns=["hylak_id", "lat", "lon"])
    result = build_grid_counts(df, resolution=0.5)
    assert len(result) == 0
    assert "geometry" in result.columns


def test_build_grid_counts_mean_per_lake():
    df = pd.DataFrame({
        "hylak_id": [1, 1, 2],
        "lat": [10.0, 10.1, 10.2],
        "lon": [20.0, 20.1, 20.0],
    })
    result = build_grid_counts(df, resolution=0.5)
    assert result["event_count"].sum() == 3
    assert result["lake_count"].sum() >= 2


def test_build_grid_counts_resolution():
    df = _make_sample_df()
    r05 = build_grid_counts(df, resolution=0.5)
    r10 = build_grid_counts(df, resolution=1.0)
    assert len(r10) <= len(r05)


def test_build_grid_counts_no_negative_means():
    df = _make_sample_df()
    result = build_grid_counts(df, resolution=0.5)
    assert (result["mean_per_lake"] >= 0).all()
