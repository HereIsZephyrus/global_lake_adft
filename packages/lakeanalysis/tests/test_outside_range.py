"""Tests for lakeanalysis.quality.compute: compute_area_range & classify_outside_range."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.quality.compute import (
    classify_outside_range,
    compute_area_range,
)


class TestComputeAreaRange:
    def test_normal_series(self):
        df = pd.DataFrame({"water_area": [10.0, 20.0, 30.0, 40.0, 50.0]})
        result = compute_area_range(df)
        assert result["min_area"] == 10.0
        assert result["max_area"] == 50.0

    def test_single_value(self):
        df = pd.DataFrame({"water_area": [42.0, 42.0, 42.0]})
        result = compute_area_range(df)
        assert result["min_area"] == 42.0
        assert result["max_area"] == 42.0

    def test_empty_dataframe(self):
        df = pd.DataFrame({"water_area": pd.Series(dtype=float)})
        result = compute_area_range(df)
        assert result["min_area"] == 0.0
        assert result["max_area"] == 0.0

    def test_with_nan_values(self):
        df = pd.DataFrame({"water_area": [10.0, np.nan, 50.0, np.nan, 30.0]})
        result = compute_area_range(df)
        assert result["min_area"] == 10.0
        assert result["max_area"] == 50.0

    def test_all_nan(self):
        df = pd.DataFrame({"water_area": [np.nan, np.nan]})
        result = compute_area_range(df)
        assert result["min_area"] == 0.0
        assert result["max_area"] == 0.0

    def test_custom_column(self):
        df = pd.DataFrame({"my_area": [5.0, 15.0]})
        result = compute_area_range(df, value_column="my_area")
        assert result["min_area"] == 5.0
        assert result["max_area"] == 15.0

    def test_negative_values(self):
        df = pd.DataFrame({"water_area": [-10.0, 0.0, 10.0]})
        result = compute_area_range(df)
        assert result["min_area"] == -10.0
        assert result["max_area"] == 10.0

    def test_large_values_m2(self):
        df = pd.DataFrame({"water_area": [31147440300.0, 32000000000.0]})
        result = compute_area_range(df)
        assert result["min_area"] == 31147440300.0
        assert result["max_area"] == 32000000000.0


class TestClassifyOutsideRange:
    def test_atlas_within_range(self):
        result = classify_outside_range(atlas_area=25.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is False
        assert result["is_below_min"] is False
        assert result["is_above_max"] is False

    def test_atlas_below_min(self):
        result = classify_outside_range(atlas_area=5.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is True
        assert result["is_below_min"] is True
        assert result["is_above_max"] is False

    def test_atlas_above_max(self):
        result = classify_outside_range(atlas_area=60.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is True
        assert result["is_below_min"] is False
        assert result["is_above_max"] is True

    def test_atlas_equals_min(self):
        result = classify_outside_range(atlas_area=10.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is False
        assert result["is_below_min"] is False
        assert result["is_above_max"] is False

    def test_atlas_equals_max(self):
        result = classify_outside_range(atlas_area=50.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is False
        assert result["is_below_min"] is False
        assert result["is_above_max"] is False

    def test_constant_series_atlas_matches(self):
        result = classify_outside_range(atlas_area=42.0, min_area=42.0, max_area=42.0)
        assert result["is_outside_range"] is False

    def test_constant_series_atlas_differs(self):
        result = classify_outside_range(atlas_area=99.0, min_area=42.0, max_area=42.0)
        assert result["is_outside_range"] is True
        assert result["is_above_max"] is True

    def test_constant_series_atlas_below(self):
        result = classify_outside_range(atlas_area=1.0, min_area=42.0, max_area=42.0)
        assert result["is_outside_range"] is True
        assert result["is_below_min"] is True

    def test_atlas_zero(self):
        result = classify_outside_range(atlas_area=0.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is False
        assert result["is_below_min"] is False
        assert result["is_above_max"] is False

    def test_atlas_negative(self):
        result = classify_outside_range(atlas_area=-5.0, min_area=10.0, max_area=50.0)
        assert result["is_outside_range"] is False
        assert result["is_below_min"] is False
        assert result["is_above_max"] is False

    def test_echo_fields(self):
        result = classify_outside_range(atlas_area=25.0, min_area=10.0, max_area=50.0)
        assert result["atlas_area"] == 25.0
        assert result["min_area"] == 10.0
        assert result["max_area"] == 50.0

    def test_min_zero_max_zero_atlas_positive(self):
        result = classify_outside_range(atlas_area=5.0, min_area=0.0, max_area=0.0)
        assert result["is_outside_range"] is True
        assert result["is_above_max"] is True

    def test_min_zero_max_positive_atlas_below(self):
        result = classify_outside_range(atlas_area=0.5, min_area=0.0, max_area=10.0)
        assert result["is_outside_range"] is False
