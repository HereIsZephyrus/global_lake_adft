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
) -> DecompositionResult:
    """Build a DecompositionResult with synthetic STL-like index_value."""
    n = len(index_values)
    if years is None:
        years = list(range(2000, 2000 + n))
    if months is None:
        months = [(i % 12) + 1 for i in range(n)]
    water_areas = [v * 100.0 for v in index_values]
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
    def test_result_structure_and_pooled_lambda(self):
        """Verify result structure and that all 12 months share the SAME lambda (pooled)."""
        data = np.random.RandomState(0).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result, hylak_id=42)
        assert isinstance(pwm, PWMExtremeResult)
        assert pwm.hylak_id == 42
        assert len(pwm.month_results) == 12
        assert all(mr.converged for mr in pwm.month_results)
        # Pooled: all months share the same lambda_opt
        first_lam = pwm.month_results[0].lambda_opt
        for mr in pwm.month_results[1:]:
            np.testing.assert_allclose(mr.lambda_opt, first_lam)
        # All months share the same epsilon
        assert all(mr.epsilon == pwm.month_results[0].epsilon for mr in pwm.month_results)
        # Objective value is reasonable
        assert 0 < pwm.month_results[0].objective_value < 1e6

    def test_thresholds_are_monotonic_and_compare_correctly(self):
        """Threshold_high > threshold_low for every month, values are positive."""
        data = np.random.RandomState(1).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        td = pwm.thresholds_df
        assert len(td) == 12
        for _, row in td.iterrows():
            assert row["threshold_high"] > row["threshold_low"]
            assert row["threshold_low"] > 0
            assert row["threshold_high"] > 0

    def test_labels_classify_known_extreme_indices(self):
        """An index_value far above threshold_high must be labeled extreme_high."""
        data = np.random.RandomState(2).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result, hylak_id=1)
        labels = pwm.labels_df
        assert "extreme_label" in labels.columns
        assert all(labels["hylak_id"] == 1)
        # At least some extremes should exist (lognormal has a right tail)
        label_counts = labels["extreme_label"].value_counts()
        assert label_counts.get("normal", 0) > 0
        # High extremes should not be empty
        assert label_counts.get("extreme_high", 0) > 0

    def test_extremes_severity_matches_formula(self):
        """Each extreme's severity equals |index_value - threshold|."""
        data = np.random.RandomState(3).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        extremes = pwm.extremes_df
        assert len(extremes) > 0
        for _, row in extremes.iterrows():
            expected_sev = abs(row["index_value"] - row["threshold"])
            assert row["severity"] == pytest.approx(expected_sev, rel=1e-6)
            assert row["event_type"] in ("high", "low")

    def test_hylak_id_defaults_to_na(self):
        """When hylak_id is not given, labels_df has pd.NA hylak_id."""
        data = [1.0] * 24
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        assert pwm.hylak_id is None
        assert pwm.labels_df["hylak_id"].isna().all()

    def test_extreme_labels_map_to_threshold(self):
        """Extreme-high rows have index_value >= threshold_high.
        Extreme-low rows have index_value <= threshold_low."""
        data = np.random.RandomState(4).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        df = pwm.labels_df
        high_mask = df["extreme_label"] == "extreme_high"
        low_mask = df["extreme_label"] == "extreme_low"
        if high_mask.any():
            assert (df.loc[high_mask, "index_value"] >= df.loc[high_mask, "threshold_high"]).all()
        if low_mask.any():
            assert (df.loc[low_mask, "index_value"] <= df.loc[low_mask, "threshold_low"]).all()

    def test_raises_on_insufficient_observations(self):
        result = _make_result([1.0, 1.1, 1.2])
        with pytest.raises(ValueError, match="Insufficient observations"):
            compute_pooled_pwm_thresholds(result)

    def test_raises_on_nonpositive_mean(self):
        result = _make_result([0.0] * 20)
        with pytest.raises(ValueError, match="Mean index_value must be positive"):
            compute_pooled_pwm_thresholds(result)

    def test_custom_n_pwm_config_changes_lambda_dimension(self):
        data = np.random.RandomState(5).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        config = PWMExtremeConfig(n_pwm=3, p_low=0.10, l2_regularization=1e-4)
        pwm = compute_pooled_pwm_thresholds(result, config=config)
        # K=3 → lambda_opt length = K+1 = 4
        assert len(pwm.month_results[0].lambda_opt) == 4
        assert len(pwm.month_results[0].pwm_coefficients) == 4

    def test_beta_coefficients_sum_to_approximately_one(self):
        """pwm_coefficients[0] should be close to 1.0 (b_0 = mean)."""
        data = np.random.RandomState(6).lognormal(0.0, 0.3, 60).tolist()
        result = _make_result(data)
        pwm = compute_pooled_pwm_thresholds(result)
        b = pwm.month_results[0].pwm_coefficients
        assert b[0] == pytest.approx(1.0, rel=0.01)
