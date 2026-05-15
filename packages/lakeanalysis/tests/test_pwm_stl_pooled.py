"""Tests for compute_pooled_pwm_thresholds — STL-based PWM on index_value."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.decomposition.base import DecompositionResult
from lakeanalysis.pwm.compute import compute_pooled_pwm_thresholds
from lakesource.pwm.schema import PWMExtremeConfig, PWMExtremeResult


def _make_result(
    index_values: list[float],
    years: list[int] | None = None,
    months: list[int] | None = None,
    hylak_id: int | None = None,
) -> DecompositionResult:
    """Build a DecompositionResult with synthetic STL-like index_value."""
    n = len(index_values)
    if years is None:
        years = list(range(2000, 2000 + n))
    if months is None:
        months = [(i % 12) + 1 for i in range(n)]
    water_areas = [v * 100.0 for v in index_values]  # dummy water_area
    df = pd.DataFrame({
        "year": years,
        "month": months,
        "water_area": water_areas,
        "year_month_key": [y * 100 + m for y, m in zip(years, months)],
        "month_ordinal": [y * 12 + (m - 1) for y, m in zip(years, months)],
        "index_value": index_values,
    })
    return DecompositionResult(index_df=df, metadata={"method": "stl_percentile"})


class TestComputePooledPWM:
    def test_basic_returns_pwm_extreme_result(self):
        data = [1.0, 1.1, 1.2, 0.9, 1.0, 1.3, 1.1, 1.2, 1.0, 0.8,
                1.1, 1.3, 1.2, 1.0, 0.9, 1.1, 1.4, 1.2, 1.0, 0.7,
                1.0, 1.2, 1.3, 1.1, 1.0, 0.9, 1.2, 1.3, 1.0, 0.8]
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result, hylak_id=42)
        assert isinstance(pwm, PWMExtremeResult)
        assert pwm.hylak_id == 42
        assert len(pwm.month_results) == 12  # one per month

    def test_all_month_results_share_same_lambda(self):
        data = np.random.RandomState(0).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        first_lam = pwm.month_results[0].lambda_opt
        for mr in pwm.month_results[1:]:
            np.testing.assert_allclose(mr.lambda_opt, first_lam)

    def test_thresholds_df_has_all_months(self):
        data = np.random.RandomState(1).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        td = pwm.thresholds_df
        assert sorted(td["month"].tolist()) == list(range(1, 13))
        assert all(td["threshold_high"] > td["threshold_low"])

    def test_labels_df_has_extreme_label_column(self):
        data = np.random.RandomState(2).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        assert "extreme_label" in pwm.labels_df.columns
        assert set(pwm.labels_df["extreme_label"].unique()).issubset(
            {"extreme_high", "extreme_low", "normal"}
        )

    def test_labels_df_includes_hylak_id_when_provided(self):
        data = [1.0] * 24
        result = _make_result(data, hylak_id=99)
        pwm = compute_pooled_pwm_thresholds(result, hylak_id=99)
        assert (pwm.labels_df["hylak_id"] == 99).all()

    def test_labels_df_has_na_hylak_id_when_not_given(self):
        data = [1.0] * 24
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        assert pwm.labels_df["hylak_id"].isna().all()

    def test_raises_on_insufficient_observations(self):
        result = _make_result([1.0, 1.1, 1.2])  # 3 < 10 default min
        with pytest.raises(ValueError, match="Insufficient observations"):
            compute_pooled_pwm_thresholds(result)

    def test_raises_on_nonpositive_mean(self):
        result = _make_result([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                               0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0])
        with pytest.raises(ValueError, match="Mean index_value must be positive"):
            compute_pooled_pwm_thresholds(result)

    def test_extremes_df_is_populated(self):
        data = [1.0, 1.1, 1.2, 0.7, 1.0, 1.5, 1.1, 1.2, 1.0, 0.6,
                1.1, 1.4, 1.2, 1.0, 0.9, 1.1, 1.3, 1.2, 1.0, 0.5,
                1.0, 1.2, 1.3, 1.1, 1.0, 0.9, 1.2, 1.3, 1.0, 0.4]
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        assert len(pwm.extremes_df) > 0
        assert "severity" in pwm.extremes_df.columns
        assert all(pwm.extremes_df["severity"] >= 0)

    def test_custom_config_respected(self):
        data = np.random.RandomState(3).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        config = PWMExtremeConfig(n_pwm=3, p_low=0.10, l2_regularization=1e-4)
        pwm = compute_pooled_pwm_thresholds(result, config=config)
        assert len(pwm.month_results[0].lambda_opt) == 4  # K+1 = 3+1

    def test_convergence_flag_is_set(self):
        data = np.random.RandomState(4).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        for mr in pwm.month_results:
            assert isinstance(mr.converged, bool)
            assert isinstance(mr.objective_value, float)
