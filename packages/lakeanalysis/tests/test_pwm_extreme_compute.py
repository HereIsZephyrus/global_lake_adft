"""Tests for lakeanalysis.pwm.compute."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.pwm.compute import (
    compute_monthly_thresholds,
    compute_one_month_thresholds,
    compute_pwm_beta,
    crossent_quantile,
    detect_pwm_abrupt_transitions,
    extract_pwm_extreme_events,
    shifted_exponential_prior,
    solve_lagrange_multipliers,
)
from lakesource.pwm.schema import PWMExtremeConfig


PAPER_EXAMPLE = np.array([
    0.52, 0.61, 0.73, 0.78, 0.82, 0.85, 0.89, 0.91, 0.94, 0.97,
    1.00, 1.03, 1.07, 1.12, 1.18, 1.24, 1.31, 1.40, 1.52, 1.68,
    1.90, 2.25, 2.80,
])


class TestComputePWMBeta:
    def test_b0_equals_mean(self):
        b = compute_pwm_beta(PAPER_EXAMPLE, K=4)
        np.testing.assert_allclose(b[0], np.mean(PAPER_EXAMPLE), rtol=1e-10)

    def test_length(self):
        b = compute_pwm_beta(PAPER_EXAMPLE, K=4)
        assert len(b) == 5

    def test_insufficient_observations(self):
        with pytest.raises(ValueError, match="Need at least"):
            compute_pwm_beta(np.array([1.0, 2.0]), K=4)


class TestShiftedExponentialPrior:
    def test_at_zero(self):
        y = shifted_exponential_prior(np.array([0.0]), epsilon=0.52)
        np.testing.assert_allclose(y, [0.52])

    def test_monotone_increasing(self):
        u = np.linspace(0, 0.99, 100)
        y = shifted_exponential_prior(u, epsilon=0.5)
        assert np.all(np.diff(y) > 0)


class TestCrossentQuantile:
    def test_reduces_to_prior_when_lambda_zero(self):
        u = np.array([0.1, 0.5, 0.9])
        lam = np.zeros(5)
        x = crossent_quantile(u, lam, epsilon=0.5)
        y = shifted_exponential_prior(u, epsilon=0.5)
        np.testing.assert_allclose(x, y, rtol=1e-10)


class TestSolveLagrangeMultipliers:
    def test_convergence(self):
        b = compute_pwm_beta(PAPER_EXAMPLE, K=4)
        lam, converged, obj = solve_lagrange_multipliers(b, K=4, epsilon=PAPER_EXAMPLE[0])
        assert converged
        assert obj < 1e-3

    def test_low_objective(self):
        b = compute_pwm_beta(PAPER_EXAMPLE, K=4)
        _, _, obj = solve_lagrange_multipliers(b, K=4, epsilon=PAPER_EXAMPLE[0])
        assert obj < 1e-4


class TestComputeOneMonthThresholds:
    def test_basic(self):
        np.random.seed(42)
        areas = 100.0 * np.random.lognormal(0, 0.2, size=23)
        mr = compute_one_month_thresholds(areas, month=7, hylak_id=1)
        assert mr.month == 7
        assert mr.hylak_id == 1
        assert mr.converged
        assert mr.threshold_high > mr.mean_area
        assert mr.threshold_low < mr.mean_area

    def test_paper_example_scale(self):
        mean_area = 50.0
        areas = PAPER_EXAMPLE * mean_area
        mr = compute_one_month_thresholds(areas, month=1)
        np.testing.assert_allclose(mr.mean_area, mean_area * np.mean(PAPER_EXAMPLE), rtol=1e-10)


class TestComputeMonthlyThresholds:
    @pytest.fixture()
    def series_df(self):
        np.random.seed(42)
        records = []
        for year in range(2000, 2023):
            for month in range(1, 13):
                base = 100.0 + 20.0 * np.sin(2 * np.pi * month / 12)
                area = base * np.random.lognormal(0, 0.2)
                records.append({"year": year, "month": month, "water_area": area})
        return pd.DataFrame(records)

    def test_all_months(self, series_df):
        config = PWMExtremeConfig(min_observations_per_month=10)
        result = compute_monthly_thresholds(series_df, hylak_id=1, config=config)
        assert len(result.month_results) == 12
        assert result.hylak_id == 1

    def test_labels_df(self, series_df):
        config = PWMExtremeConfig(min_observations_per_month=10)
        result = compute_monthly_thresholds(series_df, config=config)
        assert "extreme_label" in result.labels_df.columns
        labels = set(result.labels_df["extreme_label"].unique())
        assert labels.issubset({"extreme_low", "extreme_high", "normal"})

    def test_frozen_filter(self, series_df):
        config = PWMExtremeConfig(min_observations_per_month=10)
        frozen = {200001, 200002}
        result = compute_monthly_thresholds(series_df, config=config, frozen_year_months=frozen)
        assert len(result.month_results) == 12

    def test_empty_after_frozen(self, series_df):
        all_keys = set(series_df["year"].astype(int) * 100 + series_df["month"].astype(int))
        config = PWMExtremeConfig(min_observations_per_month=10)
        with pytest.raises(ValueError, match="No observations remain"):
            compute_monthly_thresholds(series_df, config=config, frozen_year_months=all_keys)

    def test_result_has_extremes_df(self, series_df):
        config = PWMExtremeConfig(min_observations_per_month=10)
        result = compute_monthly_thresholds(series_df, config=config)
        assert result.extremes_df is not None

    def test_result_has_transitions_df(self, series_df):
        config = PWMExtremeConfig(min_observations_per_month=10)
        result = compute_monthly_thresholds(series_df, config=config)
        assert result.transitions_df is not None


class TestExtractPWMExtremeEvents:
    def test_no_extreme_returns_empty(self):
        df = pd.DataFrame({
            "hylak_id": [1, 1],
            "year": [2000, 2000],
            "month": [1, 2],
            "water_area": [100.0, 110.0],
            "extreme_label": ["normal", "normal"],
        })
        result = extract_pwm_extreme_events(df)
        assert result.empty
        assert "severity" in result.columns

    def test_high_low_events(self):
        df = pd.DataFrame({
            "hylak_id": [1, 1, 1, 1],
            "year": [2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4],
            "water_area": [180.0, 110.0, 30.0, 100.0],
            "threshold_low": [40.0, 40.0, 40.0, 40.0],
            "threshold_high": [150.0, 150.0, 150.0, 150.0],
            "extreme_label": ["extreme_high", "normal", "extreme_low", "normal"],
        })
        result = extract_pwm_extreme_events(df)
        assert len(result) == 2
        assert set(result["event_type"]) == {"high", "low"}
        high_row = result[result["event_type"] == "high"].iloc[0]
        assert high_row["water_area"] == 180.0
        assert high_row["severity"] == 30.0
        low_row = result[result["event_type"] == "low"].iloc[0]
        assert low_row["water_area"] == 30.0
        assert low_row["severity"] == 10.0


class TestDetectPWMAbruptTransitions:
    def test_no_transition_returns_empty(self):
        df = pd.DataFrame({
            "hylak_id": [1, 1],
            "year": [2000, 2000],
            "month": [1, 2],
            "month_ordinal": [0, 1],
            "water_area": [100.0, 110.0],
            "extreme_label": ["normal", "normal"],
        })
        result = detect_pwm_abrupt_transitions(df)
        assert result.empty

    def test_low_to_high_transition(self):
        df = pd.DataFrame({
            "hylak_id": [1, 1],
            "year": [2000, 2000],
            "month": [1, 2],
            "month_ordinal": [0, 1],
            "water_area": [30.0, 180.0],
            "extreme_label": ["extreme_low", "extreme_high"],
        })
        result = detect_pwm_abrupt_transitions(df)
        assert len(result) == 1
        row = result.iloc[0]
        assert row["transition_type"] == "low_to_high"
        assert row["from_water_area"] == 30.0
        assert row["to_water_area"] == 180.0

    def test_high_to_low_transition(self):
        df = pd.DataFrame({
            "hylak_id": [1, 1],
            "year": [2000, 2000],
            "month": [1, 2],
            "month_ordinal": [0, 1],
            "water_area": [180.0, 30.0],
            "extreme_label": ["extreme_high", "extreme_low"],
        })
        result = detect_pwm_abrupt_transitions(df)
        assert len(result) == 1
        assert result.iloc[0]["transition_type"] == "high_to_low"

    def test_non_adjacent_skipped(self):
        df = pd.DataFrame({
            "hylak_id": [1, 1, 1],
            "year": [2000, 2001, 2001],
            "month": [1, 1, 2],
            "month_ordinal": [0, 12, 13],
            "water_area": [30.0, 100.0, 180.0],
            "extreme_label": ["extreme_low", "normal", "extreme_high"],
        })
        result = detect_pwm_abrupt_transitions(df)
        assert result.empty
