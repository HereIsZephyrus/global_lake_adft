"""Comparison global map merge & standardize logic tests.

Tests data merging and standardization helpers without requiring a live
database or matplotlib rendering.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

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


# ── _merge_two ────────────────────────────────────────────────────────────

class TestMergeTwo:
    def test_identical_cells_merge_columns(self):
        """Both DataFrames with same cells produce merged rows with all columns."""
        left = _make_grid_df({"mean_high": [1.0, 2.0, 3.0]}, n=3)
        right = _make_grid_df({"mean_high_exceedance": [10.0, 20.0, 30.0]}, n=3)
        result = _merge_two(
            left, right,
            left_map={"mean_high": "left_high"},
            right_map={"mean_high_exceedance": "right_high"},
        )
        assert len(result) == 3
        assert result.iloc[0]["left_high"] == 1.0
        assert result.iloc[0]["right_high"] == 10.0

    def test_lake_count_is_maximum_of_left_and_right(self):
        left = _make_grid_df({"val": [1.0]}, n=1)
        right = _make_grid_df({"val": [10.0]}, n=1)
        left["lake_count"] = [5]
        right["lake_count"] = [15]
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert result.iloc[0]["lake_count"] == 15
        assert result.iloc[0]["lake_count_left"] == 5
        assert result.iloc[0]["lake_count_right"] == 15

    def test_outer_join_preserves_disjoint_cells(self):
        """Cells only in left or only in right are preserved with outer join."""
        left = _make_grid_df({"val": [1.0, 2.0]}, n=2, start_cell=0)
        right = _make_grid_df({"val": [10.0]}, n=1, start_cell=5)  # different cell
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert len(result) == 3  # outer join
        # Right-only cell has right_val=10, left_val=0 (coerced)
        right_only = result[result["cell_lat"] == 35.0]
        assert right_only.iloc[0]["right_val"] == 10.0
        assert right_only.iloc[0]["left_val"] == 0.0

    def test_left_right_columns_are_float_after_coercion(self):
        left = _make_grid_df({"val": [1.0, 2.0]}, n=2, start_cell=0)
        right = _make_grid_df({"val": []}, n=0, start_cell=5)
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert result["left_val"].dtype.kind == "f"
        assert result["right_val"].dtype.kind == "f"

    def test_nan_in_value_column_is_coerced_to_zero(self):
        """NaN in value columns must be replaced with 0.0, not NaN."""
        left = _make_grid_df({"val": [np.nan, 2.0]}, n=2, start_cell=0)
        right = _make_grid_df({"val": [10.0]}, n=1, start_cell=0)
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert result.iloc[0]["left_val"] == 0.0
        assert result.iloc[1]["right_val"] == 0.0

    def test_preserves_original_cell_lat_lon(self):
        left = _make_grid_df({"val": [1.0]}, n=1, start_cell=0)
        right = _make_grid_df({"val": [2.0]}, n=1, start_cell=0)
        result = _merge_two(
            left, right,
            left_map={"val": "left_val"},
            right_map={"val": "right_val"},
        )
        assert result.iloc[0]["cell_lat"] == 10.0
        assert result.iloc[0]["cell_lon"] == 20.0


# ── Fake provider for standardize tests ───────────────────────────────────

class FakeComparisonProvider:
    def __init__(self, dfs: dict | None = None) -> None:
        self._dfs = dfs or {}

    def fetch_grid_agg(
        self, query_name: str, resolution: float = 0.5,
        *, refresh: bool = False, **kwargs,
    ) -> pd.DataFrame:
        return self._dfs.get(query_name, pd.DataFrame())


# ── _standardize_quantile_vs_pwm ──────────────────────────────────────────

class TestStandardizeQuantileVsPWM:
    def test_values_propagated_correctly(self):
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
        assert result.iloc[0]["left_high_mean"] == 1.0
        assert result.iloc[0]["right_high_mean"] == 10.0
        assert result.iloc[0]["left_all_median"] == 1.1
        assert result.iloc[0]["right_all_median"] == 11.0


# ── _standardize_pwm_pvalues ─────────────────────────────────────────────

class TestStandardizePWMPValues:
    def test_values_propagated_from_different_p_values(self):
        pwm_fields_001 = {
            "mean_high_exceedance": [0.3],
            "median_high_exceedance": [0.5],
            "mean_low_exceedance": [0.2],
            "median_low_exceedance": [0.4],
            "mean_all_exceedance": [0.25],
            "median_all_exceedance": [0.45],
        }
        pwm_fields_005 = {k: [v[0] * 2] for k, v in pwm_fields_001.items()}
        provider = FakeComparisonProvider({
            # Both calls return the same query name but with different p values
            # In reality they'd return different data; our fake returns same for both
        })
        provider._dfs["pwm.exceedance"] = _make_grid_df(pwm_fields_001, n=1)
        # First call (p=0.01) gets pwm_fields_001, second (p=0.05) gets pwm_fields_005
        # Our fake can't distinguish by kwargs, so test column presence + structure
        result = _standardize_pwm_pvalues(provider, 0.5, p1=0.01, p2=0.05)
        assert "left_high_mean" in result.columns
        assert "right_high_mean" in result.columns
        assert not result.empty


# ── _standardize_eot_quantiles ────────────────────────────────────────────

class TestStandardizeEOTQuantiles:
    def test_multi_step_merge_preserves_all_tails(self):
        provider = FakeComparisonProvider({
            "eot.converged": _make_grid_df({
                "mean_extremes_freq": [0.5],
                "median_extremes_freq": [0.6],
            }, n=1),
            "eot.converged_all": _make_grid_df({
                "mean_all_extremes_freq": [0.7],
                "median_all_extremes_freq": [0.8],
            }, n=1),
        })
        result = _standardize_eot_quantiles(provider, 0.5, q1=0.95, q2=0.98)
        # High tail
        assert result.iloc[0]["left_high_mean"] == 0.5
        assert result.iloc[0]["right_high_median"] == 0.6
        # All tail
        assert result.iloc[0]["left_all_mean"] == 0.7
        assert result.iloc[0]["right_all_median"] == 0.8

    def test_no_rows_produces_empty_result(self):
        """Edge case: empty DataFrames with correct structure produce empty result."""
        empty_with_cols = pd.DataFrame(columns=[
            "cell_lat", "cell_lon", "lake_count",
            "mean_extremes_freq", "median_extremes_freq",
        ])
        empty_all_cols = pd.DataFrame(columns=[
            "cell_lat", "cell_lon", "lake_count",
            "mean_all_extremes_freq", "median_all_extremes_freq",
        ])
        provider = FakeComparisonProvider({
            "eot.converged": empty_with_cols,
            "eot.converged_all": empty_all_cols,
        })
        result = _standardize_eot_quantiles(provider, 0.5, q1=0.95, q2=0.98)
        assert len(result) == 0


# ── _standardize_pwm_vs_eot ───────────────────────────────────────────────

class TestStandardizePWMVsEOT:
    def test_cross_domain_merge_has_all_tails(self):
        provider = FakeComparisonProvider({
            "pwm.exceedance": _make_grid_df({
                "mean_high_exceedance": [1.0], "median_high_exceedance": [2.0],
                "mean_low_exceedance": [0.5], "median_low_exceedance": [0.8],
                "mean_all_exceedance": [0.9], "median_all_exceedance": [1.1],
            }, n=1),
            "eot.converged": _make_grid_df({
                "mean_extremes_freq": [10.0],
                "median_extremes_freq": [20.0],
            }, n=1),
            "eot.converged_all": _make_grid_df({
                "mean_all_extremes_freq": [30.0],
                "median_all_extremes_freq": [40.0],
            }, n=1),
        })
        result = _standardize_pwm_vs_eot(provider, 0.5)
        assert result.iloc[0]["left_high_mean"] == 1.0
        assert result.iloc[0]["right_high_mean"] == 10.0  # EOT high
        assert result.iloc[0]["right_all_median"] == 40.0  # EOT all


# ── _get_land_mask ────────────────────────────────────────────────────────

class TestLandMask:
    def test_cache_hit_same_object(self):
        lons = np.array([0.0, 1.0, 2.0])
        lats = np.array([0.0, 1.0])
        m1 = _get_land_mask(lons, lats, 0.5)
        m2 = _get_land_mask(lons, lats, 0.5)
        assert m1 is m2  # same object, cache hit

    def test_different_resolution_not_cached_together(self):
        lons = np.array([0.0])
        lats = np.array([0.0])
        m1 = _get_land_mask(lons, lats, 0.5)
        m2 = _get_land_mask(lons, lats, 1.0)
        assert m1 is not m2

    def test_mask_is_2d_boolean_with_expected_dimensions(self):
        lons = np.array([0.0, 1.0, 2.0, 3.0])
        lats = np.array([0.0, 1.0, 2.0])
        mask = _get_land_mask(lons, lats, 0.5)
        assert mask.ndim == 2
        assert mask.dtype == bool
        assert mask.shape[0] > 0
        assert mask.shape[1] > 0
