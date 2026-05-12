"""Entropy plotting helpers tests (P0).

Covers remove_amplitude_outliers boundary conditions that were previously
untested.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeviz.entropy.plot import remove_amplitude_outliers


class TestRemoveAmplitudeOutliers:
    def test_no_column_returns_unchanged(self) -> None:
        df = pd.DataFrame({"other_col": [1.0, 2.0, 3.0, 4.0, 5.0]})
        result = remove_amplitude_outliers(df)
        pd.testing.assert_frame_equal(result, df)

    def test_too_few_rows_returns_unchanged(self) -> None:
        df = pd.DataFrame({
            "mean_seasonal_amplitude": [1.0, 100.0, 1.0],
        })
        result = remove_amplitude_outliers(df)
        assert len(result) == 3

    def test_zero_iqr_returns_unchanged(self) -> None:
        df = pd.DataFrame({
            "mean_seasonal_amplitude": [5.0] * 10,
        })
        result = remove_amplitude_outliers(df)
        assert len(result) == 10

    def test_removes_positive_outlier(self) -> None:
        base = [5.0, 10.0, 15.0] * 33
        df = pd.DataFrame({
            "mean_seasonal_amplitude": base + [500.0],
        })
        result = remove_amplitude_outliers(df)
        assert len(result) == 99
        assert 500.0 not in result["mean_seasonal_amplitude"].values

    def test_removes_negative_outlier(self) -> None:
        base = [5.0, 10.0, 15.0] * 33
        df = pd.DataFrame({
            "mean_seasonal_amplitude": base + [-500.0],
        })
        result = remove_amplitude_outliers(df)
        assert len(result) == 99
        assert -500.0 not in result["mean_seasonal_amplitude"].values

    def test_only_removes_outliers_not_inliers(self) -> None:
        base = [5.0, 10.0, 15.0] * 67
        df = pd.DataFrame({
            "mean_seasonal_amplitude": base + [50.0] + base[:99],
        })
        result = remove_amplitude_outliers(df)
        assert 50.0 not in result["mean_seasonal_amplitude"].values
        assert len(result) == 300

    def test_nan_values_ignored_in_outlier_calc(self) -> None:
        base = [5.0, 10.0, 15.0] * 33
        df = pd.DataFrame({
            "mean_seasonal_amplitude": base[:48] + [np.nan, np.nan] + base[:50] + [100.0],
        })
        result = remove_amplitude_outliers(df)
        assert 100.0 not in result["mean_seasonal_amplitude"].values
        assert result["mean_seasonal_amplitude"].isna().sum() == 2

    def test_all_same_value_except_one_missing(self) -> None:
        df = pd.DataFrame({
            "mean_seasonal_amplitude": [3.0] * 9 + [np.nan],
        })
        result = remove_amplitude_outliers(df)
        assert len(result) == 10

    def test_mixed_sign_values(self) -> None:
        base = [5.0, 10.0, 15.0] * 33 + [-5.0, -10.0, -15.0] * 34
        df = pd.DataFrame({
            "mean_seasonal_amplitude": base + [500.0],
        })
        result = remove_amplitude_outliers(df)
        assert 500.0 not in result["mean_seasonal_amplitude"].values
