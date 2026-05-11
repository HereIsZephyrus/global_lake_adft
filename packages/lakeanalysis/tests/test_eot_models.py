import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.basis import HarmonicBasis
from lakeanalysis.eot.models import FitResult, LocationModel, PreparedExtremes
from lakeanalysis.eot.series import MonthlyTimeSeries


def _make_series(years=2):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            rows.append({
                "year": year,
                "month": month,
                "water_area": 100.0 + 10.0 * np.sin(2 * np.pi * month / 12) + year * 2.0,
            })
    return MonthlyTimeSeries.from_frame(pd.DataFrame(rows))


class TestLocationModel:
    def test_default_construction(self):
        lm = LocationModel()
        assert lm.include_trend is True
        assert lm.n_harmonics == 1
        assert isinstance(lm.basis_model, HarmonicBasis)
        assert lm.basis_model.n_harmonics == 1

    def test_param_names_with_trend(self):
        lm = LocationModel(include_trend=True, n_harmonics=1)
        assert lm.param_names == ("beta0", "beta1", "sin_1", "cos_1")

    def test_param_names_without_trend(self):
        lm = LocationModel(include_trend=False, n_harmonics=2)
        assert lm.param_names == ("beta0", "sin_1", "cos_1", "sin_2", "cos_2")

    def test_n_params_matches_param_names(self):
        lm = LocationModel(n_harmonics=3, include_trend=True)
        assert lm.n_params == len(lm.param_names)

    def test_design_matrix_shape(self):
        lm = LocationModel(n_harmonics=1, include_trend=True)
        times = np.linspace(0, 2, 24)
        dm = lm.design_matrix(times)
        assert dm.shape == (24, lm.n_params)
        assert dm.shape[1] == 4

    def test_design_matrix_intercept_column(self):
        lm = LocationModel()
        dm = lm.design_matrix(np.array([0.0, 1.0, 2.0]))
        np.testing.assert_array_equal(dm[:, 0], np.ones(3))

    def test_design_matrix_trend_column(self):
        lm = LocationModel(include_trend=True)
        times = np.array([0.0, 1.0, 2.0])
        dm = lm.design_matrix(times)
        np.testing.assert_array_equal(dm[:, 1], times)

    def test_evaluate_matches_manual(self):
        lm = LocationModel(n_harmonics=1, include_trend=False)
        params = np.array([10.0, 2.0, 3.0])
        times = np.array([0.0])
        result = lm.evaluate(times, params)
        expected = 10.0 + 2.0 * np.sin(2 * np.pi * 0.0) + 3.0 * np.cos(2 * np.pi * 0.0)
        assert result[0] == pytest.approx(expected)

    def test_custom_basis_model(self):
        basis = HarmonicBasis(n_harmonics=2)
        lm = LocationModel(basis_model=basis)
        assert lm.basis_model is basis
        assert lm.basis_model.n_harmonics == 2

    def test_evaluate_output_shape(self):
        lm = LocationModel(n_harmonics=2, include_trend=True)
        params = np.ones(lm.n_params)
        times = np.linspace(0, 5, 60)
        result = lm.evaluate(times, params)
        assert result.shape == (60,)
        assert result.dtype == float


class TestFitResult:
    @staticmethod
    def _minimal_fit_result(tail="high", threshold=80.0, theta=None):
        series = _make_series()
        lm = LocationModel(n_harmonics=1, include_trend=True)
        if theta is None:
            theta = np.array([10.0, 1.0, 2.0, 3.0, 5.0, 0.1])
        return FitResult(
            theta=theta,
            covariance=np.eye(len(theta)),
            threshold=threshold,
            tail=tail,
            log_likelihood=-123.45,
            converged=True,
            message="ok",
            location_model=lm,
            series=series,
            extremes=pd.DataFrame({
                "year": [2000, 2000],
                "month": [6, 12],
                "time": [0.416667, 0.916667],
                "value": [130.0, 120.0],
                "original_value": [130.0, 120.0],
                "threshold": [100.0, 100.0],
                "cluster_id": [1, 2],
                "cluster_size": [1, 1],
            }),
        )

    def test_param_names_end_with_sigma_xi(self):
        fr = self._minimal_fit_result()
        names = fr.param_names
        assert names[-2] == "sigma"
        assert names[-1] == "xi"

    def test_params_dict_matches_theta(self):
        theta = np.array([10.0, 1.0, 2.0, 3.0, 5.0, 0.1])
        fr = self._minimal_fit_result(theta=theta)
        params = fr.params
        assert params["sigma"] == pytest.approx(5.0)
        assert params["xi"] == pytest.approx(0.1)

    def test_sigma_property(self):
        fr = self._minimal_fit_result(theta=np.array([10.0, 1.0, 2.0, 3.0, 5.0, 0.1]))
        assert fr.sigma == pytest.approx(5.0)

    def test_xi_property(self):
        fr = self._minimal_fit_result(theta=np.array([10.0, 1.0, 2.0, 3.0, 5.0, 0.2]))
        assert fr.xi == pytest.approx(0.2)

    def test_mu_returns_same_shape_as_times(self):
        fr = self._minimal_fit_result()
        times = np.linspace(0, 2, 10)
        mu_vals = fr.mu(times)
        assert mu_vals.shape == (10,)
        assert np.all(np.isfinite(mu_vals))

    def test_threshold_at_returns_constant_when_no_model(self):
        fr = self._minimal_fit_result(threshold=75.0)
        times = np.linspace(0, 2, 5)
        u_vals = fr.threshold_at(times)
        np.testing.assert_array_equal(u_vals, np.full(5, 75.0))

    def test_with_theta_returns_copy_with_different_theta(self):
        fr = self._minimal_fit_result()
        new_theta = np.array([5.0, 0.5, 1.0, 1.5, 3.0, -0.1])
        new_fr = fr.with_theta(new_theta)
        np.testing.assert_array_equal(new_fr.theta, new_theta)
        np.testing.assert_array_equal(fr.theta, np.array([10.0, 1.0, 2.0, 3.0, 5.0, 0.1]))

    def test_with_theta_preserves_other_fields(self):
        fr = self._minimal_fit_result()
        new_fr = fr.with_theta(np.array([5.0, 0.5, 1.0, 1.5, 3.0, -0.1]))
        assert new_fr.threshold == fr.threshold
        assert new_fr.tail == fr.tail
        assert new_fr.log_likelihood == fr.log_likelihood
        assert new_fr.converged == fr.converged
        assert new_fr.location_model is fr.location_model
        np.testing.assert_array_equal(new_fr.covariance, fr.covariance)

    def test_log_likelihood_preserved(self):
        fr = self._minimal_fit_result()
        assert fr.log_likelihood == pytest.approx(-123.45)


class TestPreparedExtremes:
    def test_construction(self):
        series = _make_series()
        basis = HarmonicBasis(n_harmonics=1)
        extremes = pd.DataFrame({"cluster_id": [1], "cluster_size": [1]})
        pe = PreparedExtremes(
            series=series,
            representative_threshold=80.0,
            extremes=extremes,
            basis_model=basis,
            threshold_model=None,
            threshold_params=None,
            u_grid=None,
        )
        assert pe.representative_threshold == 80.0
        assert pe.series is series
        assert pe.basis_model is basis
        assert pe.threshold_model is None
        assert pe.threshold_params is None
        assert pe.u_grid is None
