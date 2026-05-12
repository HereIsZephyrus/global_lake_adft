"""Comparison global map merge & standardize logic tests (P1).

Tests data merging and standardization helpers without requiring a live
database or matplotlib rendering.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeviz.comparison.global_map import (
    _merge_two,
    _standardize_eot_quantiles,
    _standardize_pwm_pvalues,
    _standardize_pwm_vs_eot,
    _standardize_quantile_vs_pwm,
    _get_land_mask,
)


def _make_grid_df(
    cols: dict[str, list[float]],
    n: int = 5,
    start_cell: int = 0,
) -> pd.DataFrame:
    base = {
        "cell_lat": [float(10 + i * 5) for i in range(start_cell, start_cell + n)],
        "cell_lon": [float(20 + i * 5) for i in range(start_cell, start_cell + n)],
        "lake_count": [10] * n,
    }
    base.update({k: list(v) for k, v in cols.items()})
    return pd.DataFrame(base)


class TestMergeTwo:
    def test_basic_merge(self) -> None:
        left = _make_grid_df({"mean_high": [1.0, 2.0, 3.0]}, n=3)
        right = _make_grid_df({"mean_high_exceedance": [10.0, 20.0, 30.0]}, n=3)
        result = _merge_two(
            left, right,
            left_map={"mean_high": "left_high"},
            right_map={"mean_high_exceedance": "right_high"},
        )
        assert "left_high" in result.columns
        assert "right_high" in result.columns
        assert "lake_count" in result.columns
        assert "lake_count_left" in result.columns
        assert "lake_count_right" in result.columns

    def test_empty_right_produces_nans_in_right_columns(self) -> None:
        left = _make_grid_df({"val": [1.0, 2.0]}, n=2, start_cell=0)
        right = _make_grid_df({"val": []}, n=0, start_cell=5)  # empty, different cells
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert len(result) == 2  # left cells preserved
        assert (result["right_val"].fillna(0) == 0).all()

    def test_lake_count_is_maximum(self) -> None:
        left = _make_grid_df({"val": [1.0]}, n=1, start_cell=0)
        right = _make_grid_df({"val": [10.0]}, n=1, start_cell=0)
        left["lake_count"] = [5]
        right["lake_count"] = [15]
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert result.iloc[0]["lake_count"] == 15

    def test_left_right_cols_numeric_coerced(self) -> None:
        left = _make_grid_df({"val": [1.0, 2.0]}, n=2, start_cell=0)
        right = _make_grid_df({"val": []}, n=0, start_cell=5)
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert result["left_val"].dtype.kind == "f"
        assert result["right_val"].dtype.kind == "f"


class FakeComparisonProvider:
    def __init__(self, dfs: dict | None = None) -> None:
        self._dfs = dfs or {}

    def fetch_grid_agg(self, query_name: str, resolution: float = 0.5, *, refresh: bool = False, **kwargs) -> pd.DataFrame:
        return self._dfs.get(query_name, pd.DataFrame())


class TestStandardizeQuantileVsPWM:
    def test_returns_expected_columns(self) -> None:
        provider = FakeComparisonProvider({
            "quantile.per_lake_stats": _make_grid_df({
                "mean_high": [1.0], "median_high": [2.0],
                "mean_low": [0.5], "median_low": [0.8],
                "mean_all": [0.9], "median_all": [1.1],
            }, n=1),
            "pwm.exceedance": _make_grid_df({
                "mean_high_exceedance": [10.0], "median_high_exceedance": [20.0],
                "mean_low_exceedance": [5.0], "median_low_exceedance": [8.0],
                "mean_all_exceedance": [9.0], "median_all_exceedance": [11.0],
            }, n=1),
        })
        result = _standardize_quantile_vs_pwm(provider, 0.5)
        assert "left_high_mean" in result.columns
        assert "right_high_mean" in result.columns
        assert "left_all_median" in result.columns
        assert "right_all_median" in result.columns
        assert not result.empty


class TestStandardizePWMPValues:
    def test_returns_expected_columns(self) -> None:
        pwm_fields = {
            "mean_high_exceedance": [1.0],
            "median_high_exceedance": [2.0],
            "mean_low_exceedance": [0.5],
            "median_low_exceedance": [0.8],
            "mean_all_exceedance": [0.9],
            "median_all_exceedance": [1.1],
        }
        provider = FakeComparisonProvider({
            "pwm.exceedance": _make_grid_df(pwm_fields, n=1),  # same df for both calls
        })
        result = _standardize_pwm_pvalues(provider, 0.5, p1=0.01, p2=0.05)
        assert "left_high_mean" in result.columns
        assert "right_high_mean" in result.columns
        assert "left_all_median" in result.columns
        assert "right_all_median" in result.columns


class TestStandardizeEOTQuantiles:
    def test_returns_expected_columns(self) -> None:
        def _eot_df() -> pd.DataFrame:
            return _make_grid_df({
                "mean_extremes_freq": [0.5],
                "median_extremes_freq": [0.6],
            }, n=1)

        provider = FakeComparisonProvider({
            "eot.converged": _eot_df(),
            "eot.converged_all": _make_grid_df({
                "mean_all_extremes_freq": [0.7],
                "median_all_extremes_freq": [0.8],
            }, n=1),
        })
        result = _standardize_eot_quantiles(provider, 0.5, q1=0.95, q2=0.98)
        assert "left_high_mean" in result.columns
        assert "right_high_mean" in result.columns
        assert "left_low_mean" in result.columns
        assert "right_low_mean" in result.columns
        assert "left_all_mean" in result.columns
        assert "right_all_mean" in result.columns


class TestStandardizePWMVsEOT:
    def test_returns_expected_columns(self) -> None:
        provider = FakeComparisonProvider({
            "pwm.exceedance": _make_grid_df({
                "mean_high_exceedance": [1.0], "median_high_exceedance": [2.0],
                "mean_low_exceedance": [0.5], "median_low_exceedance": [0.8],
                "mean_all_exceedance": [0.9], "median_all_exceedance": [1.1],
            }, n=1),
            "eot.converged": _make_grid_df({
                "mean_extremes_freq": [0.5],
                "median_extremes_freq": [0.6],
            }, n=1),
            "eot.converged_all": _make_grid_df({
                "mean_all_extremes_freq": [0.7],
                "median_all_extremes_freq": [0.8],
            }, n=1),
        })
        result = _standardize_pwm_vs_eot(provider, 0.5)
        assert "left_high_mean" in result.columns
        assert "right_high_mean" in result.columns
        assert "left_low_mean" in result.columns
        assert "right_low_mean" in result.columns
        assert "left_all_median" in result.columns
        assert "right_all_median" in result.columns


class TestLandMask:
    def test_cache_hit(self) -> None:
        lons = np.array([0.0, 1.0, 2.0])
        lats = np.array([0.0, 1.0])
        m1 = _get_land_mask(lons, lats, 0.5)
        m2 = _get_land_mask(lons, lats, 0.5)
        assert m1 is m2  # same object, cache hit

    def test_different_resolution_different_cache(self) -> None:
        lons = np.array([0.0])
        lats = np.array([0.0])
        m1 = _get_land_mask(lons, lats, 0.5)
        m2 = _get_land_mask(lons, lats, 1.0)
        assert m1 is not m2  # different resolutions must NOT share cache
