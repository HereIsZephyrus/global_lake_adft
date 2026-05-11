import numpy as np
import pandas as pd
import pytest

from lakeanalysis.entropy.compute import (
    MIN_YEARS_TREND,
    ae_from_values,
    compute_annual_ae,
    compute_overall_ae,
    compute_trend,
)


class TestAeFromValues:
    def test_uniform_distribution(self):
        ae = ae_from_values(np.array([1.0, 1.0, 1.0]))
        assert ae == pytest.approx(np.log2(3), rel=1e-9)

    def test_uniform_distribution_large(self):
        ae = ae_from_values(np.array([5.0, 5.0, 5.0, 5.0]))
        assert ae == pytest.approx(np.log2(4), rel=1e-9)

    def test_single_nonzero(self):
        ae = ae_from_values(np.array([1.0, 0.0, 0.0]))
        assert ae == pytest.approx(0.0, abs=1e-9)

    def test_two_values(self):
        ae = ae_from_values(np.array([1.0, 2.0]))
        expected = -((1 / 3) * np.log2(1 / 3) + (2 / 3) * np.log2(2 / 3))
        assert ae == pytest.approx(expected, rel=1e-9)

    def test_all_zeros(self):
        ae = ae_from_values(np.array([0.0, 0.0, 0.0]))
        assert np.isnan(ae)

    def test_total_zero(self):
        ae = ae_from_values(np.array([]))
        assert np.isnan(ae)

    def test_negative_values_not_nan(self):
        ae = ae_from_values(np.array([-1.0, 2.0]))
        assert not np.isnan(ae)

    def test_negative_and_positive_sum_zero(self):
        ae = ae_from_values(np.array([1.0, -1.0]))
        assert np.isnan(ae)

    def test_mixed_with_zeros(self):
        ae = ae_from_values(np.array([3.0, 0.0, 1.0]))
        total = 4.0
        expected = -((3 / 4) * np.log2(3 / 4) + (1 / 4) * np.log2(1 / 4))
        assert ae == pytest.approx(expected, rel=1e-9)

    def test_max_entropy_12_months(self):
        values = np.ones(12)
        assert ae_from_values(values) == pytest.approx(np.log2(12), rel=1e-9)

    def test_single_value(self):
        ae = ae_from_values(np.array([5.0]))
        assert ae == pytest.approx(0.0, abs=1e-9)


class TestComputeOverallAe:
    def test_uniform_seasonal_cycle(self):
        df = pd.DataFrame({
            "month": list(range(1, 13)) * 2,
            "water_area": [10.0] * 24,
        })
        ae = compute_overall_ae(df)
        assert np.isnan(ae)

    def test_single_month_variation(self):
        areas = [5.0] * 11 + [10.0]
        df = pd.DataFrame({
            "month": list(range(1, 13)) * 2,
            "water_area": areas * 2,
        })
        ae = compute_overall_ae(df)
        assert ae == pytest.approx(0.0, abs=1e-9)

    def test_two_season(self):
        areas = [10.0] * 6 + [20.0] * 6
        df = pd.DataFrame({
            "month": list(range(1, 13)) * 3,
            "water_area": areas * 3,
        })
        ae = compute_overall_ae(df)
        assert 0.6 < ae < 0.85

    def test_partial_year_data(self):
        df = pd.DataFrame({
            "month": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "water_area": [
                10.0, 12.0, 15.0, 18.0, 20.0, 22.0,
                22.0, 20.0, 18.0, 15.0, 12.0, 10.0,
            ],
        })
        ae = compute_overall_ae(df)
        assert 0.85 < ae < 1.05

    def test_high_baseline(self):
        areas = [1000.0] * 6 + [1010.0] * 6
        df = pd.DataFrame({
            "month": list(range(1, 13)) * 2,
            "water_area": areas * 2,
        })
        ae = compute_overall_ae(df)
        assert 0.6 < ae < 0.85


class TestComputeAnnualAe:
    def test_single_year(self):
        df = pd.DataFrame({
            "year": [2000] * 12,
            "month": list(range(1, 13)),
            "water_area": [10.0] * 6 + [20.0] * 6,
        })
        annual = compute_annual_ae(df)
        assert len(annual) == 1
        assert annual.iloc[0]["year"] == 2000
        assert 0.6 < annual.iloc[0]["AE"] < 0.85
        assert annual.iloc[0]["AE_anomaly"] == pytest.approx(0.0)

    def test_multiple_years(self):
        areas = [10.0] * 6 + [20.0] * 6
        df = pd.DataFrame({
            "year": [2000] * 12 + [2001] * 12 + [2002] * 12,
            "month": list(range(1, 13)) * 3,
            "water_area": areas * 3,
        })
        annual = compute_annual_ae(df)
        assert len(annual) == 3
        assert list(annual["year"]) == [2000, 2001, 2002]
        assert np.allclose(annual["AE_anomaly"], 0, atol=1e-9)

    def test_insufficient_months_filtered(self):
        df = pd.DataFrame({
            "year": [2000] * 12 + [2001] * 5,
            "month": list(range(1, 13)) + list(range(1, 6)),
            "water_area": [10.0] * 6 + [20.0] * 6 + [10.0] * 5,
        })
        annual = compute_annual_ae(df, min_months=10)
        assert len(annual) == 1

    def test_custom_min_months(self):
        df = pd.DataFrame({
            "year": [2000] * 8,
            "month": list(range(1, 9)),
            "water_area": [5.0] * 4 + [15.0] * 4,
        })
        annual = compute_annual_ae(df, min_months=8)
        assert len(annual) == 1

    def test_custom_min_months_filters(self):
        df = pd.DataFrame({
            "year": [2000] * 8,
            "month": list(range(1, 9)),
            "water_area": [10.0] * 8,
        })
        annual = compute_annual_ae(df, min_months=9)
        assert len(annual) == 0

    def test_all_years_filtered(self):
        df = pd.DataFrame({
            "year": [2000] * 5,
            "month": list(range(1, 6)),
            "water_area": [10.0] * 5,
        })
        annual = compute_annual_ae(df, min_months=10)
        assert len(annual) == 0
        assert list(annual.columns) == ["year", "AE", "AE_anomaly"]

    def test_empty_df(self):
        df = pd.DataFrame(columns=["year", "month", "water_area"])
        annual = compute_annual_ae(df)
        assert len(annual) == 0

    def test_all_zero_area(self):
        df = pd.DataFrame({
            "year": [2000] * 12,
            "month": list(range(1, 13)),
            "water_area": [0.0] * 12,
        })
        annual = compute_annual_ae(df)
        assert len(annual) == 0

    def test_varying_ae_across_years(self):
        areas_2000 = [10.0] * 6 + [20.0] * 6
        areas_2001 = [10.0] * 1 + [20.0] * 11
        areas_2002 = [10.0] * 3 + [20.0] * 3 + [30.0] * 3 + [15.0] * 3
        df = pd.DataFrame({
            "year": [2000] * 12 + [2001] * 12 + [2002] * 12,
            "month": list(range(1, 13)) * 3,
            "water_area": areas_2000 + areas_2001 + areas_2002,
        })
        annual = compute_annual_ae(df)
        assert len(annual) == 3
        assert not np.isnan(annual.iloc[0]["AE"])
        assert not np.isnan(annual.iloc[1]["AE"])
        assert not np.isnan(annual.iloc[2]["AE"])

    def test_ae_anomaly_nonzero(self):
        areas_2000 = [10.0] * 6 + [20.0] * 6
        areas_2001 = [10.0] * 9 + [20.0] * 3
        df = pd.DataFrame({
            "year": [2000] * 12 + [2001] * 12,
            "month": list(range(1, 13)) * 2,
            "water_area": areas_2000 + areas_2001,
        })
        annual = compute_annual_ae(df)
        assert len(annual) == 2
        assert not np.allclose(annual["AE_anomaly"], 0)


class TestComputeTrend:
    def _make_annual(self, years, ae_values):
        ae_anomaly = [v - np.mean(ae_values) for v in ae_values]
        return pd.DataFrame({
            "year": years,
            "AE": ae_values,
            "AE_anomaly": ae_anomaly,
        })

    def test_sufficient_years(self):
        n = MIN_YEARS_TREND + 1
        ae = np.linspace(0.5, 0.7, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["n_years"] == n
        assert result["sens_slope"] is not None
        assert result["change_per_decade_pct"] is not None
        assert result["mk_trend"] is not None
        assert result["mk_p"] is not None
        assert result["mk_z"] is not None
        assert result["mk_significant"] is not None

    def test_insufficient_years(self):
        n = MIN_YEARS_TREND - 1
        ae = np.linspace(0.5, 0.7, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["n_years"] == n
        assert result["sens_slope"] is None
        assert result["change_per_decade_pct"] is None
        assert result["mk_trend"] is None
        assert result["mk_p"] is None
        assert result["mk_z"] is None
        assert result["mk_significant"] is None

    def test_exactly_min_years(self):
        n = MIN_YEARS_TREND
        ae = np.linspace(0.5, 0.7, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["sens_slope"] is not None

    def test_empty_df(self):
        annual = pd.DataFrame(columns=["year", "AE", "AE_anomaly"])
        result = compute_trend(annual)
        assert result["n_years"] == 0
        assert result["sens_slope"] is None

    def test_increasing_trend(self):
        n = MIN_YEARS_TREND + 5
        ae = np.linspace(0.3, 0.7, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["sens_slope"] > 0
        assert result["mk_trend"] == "increasing"

    def test_decreasing_trend(self):
        n = MIN_YEARS_TREND + 5
        ae = np.linspace(0.7, 0.3, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["sens_slope"] < 0
        assert result["mk_trend"] == "decreasing"

    def test_flat_trend(self):
        n = MIN_YEARS_TREND + 5
        ae = np.full(n, 0.5)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["sens_slope"] == pytest.approx(0.0, abs=1e-10)
        assert result["change_per_decade_pct"] == pytest.approx(0.0, abs=1e-10)

    def test_nan_filtering(self):
        n = MIN_YEARS_TREND + 5
        ae = [0.5] * n
        ae[3] = float("nan")
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["n_years"] == n - 1

    def test_nonzero_mean_ae(self):
        n = MIN_YEARS_TREND + 1
        ae = np.linspace(0.3, 0.5, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert result["change_per_decade_pct"] is not None
        assert result["sens_slope"] != 0

    def test_mk_significant_field(self):
        n = MIN_YEARS_TREND + 10
        ae = np.linspace(0.3, 0.5, n) + np.random.default_rng(42).normal(0, 0.01, n)
        annual = self._make_annual(list(range(2000, 2000 + n)), ae)
        result = compute_trend(annual)
        assert isinstance(result["mk_significant"], bool)
