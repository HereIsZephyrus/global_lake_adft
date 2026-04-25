"""Tests for lakeviz grid binning logic."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeviz.grid import build_grid_counts, agg_to_grid_matrix


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


def test_build_grid_counts_lat_uses_lat_bins_not_lon_bins():
    df = pd.DataFrame({
        "hylak_id": [1],
        "lat": [10.25],
        "lon": [20.25],
    })
    result = build_grid_counts(df, resolution=0.5)
    polygon = result.iloc[0]["geometry"]
    bounds = polygon.bounds
    assert bounds[1] >= -90
    assert bounds[3] <= 90
    assert bounds[1] != bounds[0]


def test_agg_to_grid_matrix_basic():
    agg_df = pd.DataFrame({
        "cell_lat": [10.25],
        "cell_lon": [20.25],
        "value": [5.0],
    })
    lons, lats, values = agg_to_grid_matrix(agg_df, "value", resolution=0.5)
    assert lons.shape[0] == int(360 / 0.5)
    assert lats.shape[0] == int(180 / 0.5)
    assert values.shape == (lats.shape[0], lons.shape[0])
    assert not np.all(np.isnan(values))


def test_agg_to_grid_matrix_empty():
    agg_df = pd.DataFrame(columns=["cell_lat", "cell_lon", "value"])
    lons, lats, values = agg_to_grid_matrix(agg_df, "value", resolution=0.5)
    assert np.all(np.isnan(values))
