import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.diagnostics import ModelChecker, ReturnLevelEstimator
from lakeanalysis.eot.estimation import EOTEstimator
from lakeanalysis.eot.models import FitResult, LocationModel
from lakeanalysis.eot.series import MonthlyTimeSeries


def _make_series(years=3):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            val = 100.0 + 10.0 * np.sin(2 * np.pi * month / 12) + year * 2.0
            rows.append({"year": year, "month": month, "water_area": val})
    return MonthlyTimeSeries.from_frame(pd.DataFrame(rows))


def _make_fit_result(series=None, tail="high"):
    if series is None:
        series = _make_series(years=5)
    lm = LocationModel(n_harmonics=1, include_trend=True)
    theta = np.array([105.0, 2.0, 5.0, 3.0, 8.0, 0.05])
    extremes = pd.DataFrame({
        "year": [2000, 2000, 2001, 2002, 2003],
        "month": [6, 12, 6, 7, 8],
        "time": np.linspace(0.4, 4.0, 5),
        "value": np.array([115.0, 112.0, 118.0, 120.0, 117.0], dtype=float),
        "original_value": np.array([115.0, 112.0, 118.0, 120.0, 117.0], dtype=float),
        "cluster_id": [1, 2, 3, 4, 5],
        "cluster_size": [1, 1, 1, 1, 1],
        "threshold": np.full(5, 100.0, dtype=float),
    })
    return FitResult(
        theta=theta,
        covariance=np.diag([0.1] * len(theta)),
        threshold=100.0,
        tail=tail,
        log_likelihood=-50.0,
        converged=True,
        message="ok",
        location_model=lm,
        series=series,
        extremes=extremes,
    )


class TestModelChecker:
    def test_transformed_residuals_returns_array(self):
        fr = _make_fit_result()
        checker = ModelChecker(fit_result=fr)
        residuals = checker.transformed_residuals()
        assert isinstance(residuals, np.ndarray)
        assert len(residuals) == len(fr.extremes)

    def test_transformed_residuals_are_sorted(self):
        fr = _make_fit_result()
        checker = ModelChecker(fit_result=fr)
        residuals = checker.transformed_residuals()
        assert np.all(np.diff(residuals) >= 0)

    def test_probability_plot_data_bounds(self):
        fr = _make_fit_result()
        checker = ModelChecker(fit_result=fr)
        pp_data = checker.probability_plot_data()
        assert "empirical_probability" in pp_data.columns
        assert "model_probability" in pp_data.columns
        assert pp_data["empirical_probability"].between(0, 1).all()
        assert pp_data["model_probability"].between(0, 1).all()

    def test_quantile_plot_data_is_monotonic(self):
        fr = _make_fit_result()
        checker = ModelChecker(fit_result=fr)
        qq_data = checker.quantile_plot_data()
        assert "theoretical_quantile" in qq_data.columns
        assert "empirical_quantile" in qq_data.columns
        assert np.all(np.diff(qq_data["theoretical_quantile"]) > 0)


class TestReturnLevelEstimator:
    def test_estimate_returns_dataframe_with_expected_columns(self):
        fr = _make_fit_result()
        rle = ReturnLevelEstimator(fit_result=fr)
        result = rle.estimate()
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 4
        assert "return_period_years" in result.columns
        assert "return_level" in result.columns
        assert "ci_lower" in result.columns
        assert "ci_upper" in result.columns

    def test_estimate_one_high_tail_returns_positive_level(self):
        fr = _make_fit_result(tail="high")
        rle = ReturnLevelEstimator(fit_result=fr)
        entry = rle.estimate_one(100.0)
        assert entry["return_period_years"] == 100.0
        assert entry["return_level"] > 0

    def test_estimate_one_low_tail_is_negative(self):
        fr = _make_fit_result(tail="low")
        rle = ReturnLevelEstimator(fit_result=fr)
        entry = rle.estimate_one(100.0)
        assert entry["return_level"] < 0

    def test_invalid_bracket_boundary(self):
        fr = _make_fit_result()
        fr = fr.with_theta(np.array([10.0, 1.0, 2.0, 3.0, 5.0, -0.5]))
        rle = ReturnLevelEstimator(fit_result=fr)
        entry = rle.estimate_one(5.0)
        assert isinstance(entry, dict)
        assert "return_level" in entry

    def test_estimate_default_periods(self):
        fr = _make_fit_result()
        rle = ReturnLevelEstimator(fit_result=fr)
        result = rle.estimate()
        periods = set(result["return_period_years"].tolist())
        assert periods == {10.0, 25.0, 50.0, 100.0}
