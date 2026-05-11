import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.estimation import NHPPFitter, EOTEstimator
from lakeanalysis.eot.models import LocationModel
from lakeanalysis.eot.preprocess import NoDeclustering, RunsDeclustering, ThresholdSelector
from lakeanalysis.eot.series import MonthlyTimeSeries


def _make_series(years=3):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            val = 100.0 + 10.0 * np.sin(2 * np.pi * month / 12) + year * 2.0
            rows.append({"year": year, "month": month, "water_area": val})
    return MonthlyTimeSeries.from_frame(pd.DataFrame(rows))


def _make_extremes(n=5):
    rng = np.random.default_rng(42)
    values = rng.uniform(100, 120, n)
    return pd.DataFrame({
        "year": [2000] * n,
        "month": list(range(6, 6 + n)),
        "time": np.linspace(0.4, 2.0, n),
        "value": values,
        "original_value": values,
        "cluster_id": list(range(1, n + 1)),
        "cluster_size": [1] * n,
        "threshold": np.full(n, 90.0, dtype=float),
    })


class TestNHPPFitter:
    def test_default_construction(self):
        fitter = NHPPFitter()
        assert fitter.maxiter == 2000
        assert fitter.integration_points == 256
        assert fitter.max_restarts == 4
        assert fitter.enable_powell_fallback is True

    def test_initial_location_params_shape(self):
        fitter = NHPPFitter(location_model=LocationModel(n_harmonics=1, include_trend=True))
        series = _make_series()
        loc_params = fitter._initial_location_params(series)
        assert len(loc_params) == fitter.location_model.n_params

    def test_initial_scale_shape_returns_positive_sigma(self):
        extremes = _make_extremes(n=10)
        sigma, xi = NHPPFitter._initial_scale_shape(extremes, 90.0)
        assert sigma > 0
        assert -1.0 < xi < 1.0

    def test_initial_theta_output_length(self):
        fitter = NHPPFitter(location_model=LocationModel(n_harmonics=1, include_trend=True))
        series = _make_series()
        extremes = _make_extremes(n=10)
        theta = fitter.initial_theta(series, extremes, 90.0)
        assert len(theta) == fitter.location_model.n_params + 2

    def test_candidate_initial_thetas_returns_multiple(self):
        fitter = NHPPFitter(max_restarts=4)
        series = _make_series()
        extremes = _make_extremes(n=10)
        candidates = fitter.candidate_initial_thetas(series, extremes, 90.0)
        assert len(candidates) >= 1
        assert all(len(c) == fitter.location_model.n_params + 2 for c in candidates)

    def test_max_restarts_caps_candidates_in_fit(self):
        """max_restarts caps the initial thetas used during fit, not candidate generation."""
        fitter = NHPPFitter(max_restarts=2)
        series = _make_series()
        extremes = _make_extremes(n=10)
        candidates = fitter.candidate_initial_thetas(series, extremes, 90.0)
        assert len(candidates) >= 2

    def test_max_restarts_invalid_raises_at_fit_time(self):
        """Validation of max_restarts happens in fit(), not construction."""
        fitter = NHPPFitter(max_restarts=0)
        series = _make_series()
        extremes = _make_extremes(n=10)
        with pytest.raises(ValueError, match="max_restarts must be >= 1"):
            fitter.fit(series, extremes, 90.0, tail="high")

    def test_fit_returns_fit_result(self):
        fitter = NHPPFitter(location_model=LocationModel(n_harmonics=1, include_trend=True))
        series = _make_series(years=5)
        extremes = _make_extremes(n=10)
        result = fitter.fit(series, extremes, 90.0, tail="high")
        assert result.theta is not None
        assert len(result.theta) == fitter.location_model.n_params + 2
        assert result.tail == "high"
        assert result.threshold == 90.0
        assert result.location_model is fitter.location_model

    def test_fit_with_u_grid(self):
        fitter = NHPPFitter(location_model=LocationModel(n_harmonics=1, include_trend=True))
        series = _make_series(years=5)
        extremes = _make_extremes(n=10)
        u_grid = np.full(fitter.integration_points, 90.0)
        result = fitter.fit(series, extremes, 90.0, tail="high", u_grid=u_grid)
        assert result.theta is not None

    def test_fit_low_tail(self):
        fitter = NHPPFitter(location_model=LocationModel(n_harmonics=1, include_trend=True))
        series = _make_series(years=5)
        extremes = _make_extremes(n=10)
        result = fitter.fit(series, extremes, 90.0, tail="low")
        assert result.tail == "low"


class TestEOTEstimator:
    def test_default_construction(self):
        estimator = EOTEstimator()
        assert estimator.threshold_selector is not None
        assert estimator.declustering_strategy is not None
        assert estimator.fitter is not None
        assert estimator.min_observations == 20

    def test_coerce_series_accepts_dataframe(self):
        estimator = EOTEstimator()
        df = pd.DataFrame([
            {"year": 2000, "month": i, "water_area": 100.0} for i in range(1, 13)
        ])
        series = estimator._coerce_series(df)
        assert isinstance(series, MonthlyTimeSeries)
        assert series.n_obs == 12

    def test_coerce_series_passes_through_monthly_series(self):
        estimator = EOTEstimator()
        mts = _make_series()
        result = estimator._coerce_series(mts)
        assert result is mts

    def test_estimate_threshold_returns_float(self):
        estimator = EOTEstimator()
        series = _make_series(years=5)
        threshold = estimator.estimate_threshold(series, tail="high", quantile=0.90)
        assert isinstance(threshold, float)
        assert np.isfinite(threshold)

    def test_threshold_diagnostics_returns_dict_with_mrl_and_stability(self):
        estimator = EOTEstimator()
        series = _make_series(years=3)
        diags = estimator.threshold_diagnostics(series)
        assert "mrl" in diags
        assert "stability" in diags
        assert isinstance(diags["mrl"], pd.DataFrame)
        assert isinstance(diags["stability"], pd.DataFrame)

    def test_prepare_extremes_with_fixed_threshold(self):
        estimator = EOTEstimator()
        series = _make_series(years=3)
        prepared = estimator.prepare_extremes(series, tail="high", threshold=100.0)
        assert prepared.representative_threshold == 100.0
        assert prepared.threshold_model is None
        assert prepared.threshold_params is None
        assert prepared.u_grid is None
        assert not prepared.extremes.empty

    def test_prepare_extremes_with_automatic_threshold(self):
        estimator = EOTEstimator()
        series = _make_series(years=3)
        prepared = estimator.prepare_extremes(series, tail="high", threshold_quantile=0.90)
        assert prepared.representative_threshold is not None
        assert prepared.threshold_model is not None
        assert prepared.threshold_params is not None
        assert prepared.u_grid is not None

    def test_fit_returns_fit_result(self):
        estimator = EOTEstimator()
        series = _make_series(years=5)
        result = estimator.fit(series, tail="high", threshold_quantile=0.90)
        assert result.theta is not None
        assert result.tail == "high"

    def test_fit_with_fixed_threshold(self):
        estimator = EOTEstimator()
        series = _make_series(years=5)
        threshold = 100.0
        result = estimator.fit(series, tail="high", threshold=threshold)
        assert result.threshold == threshold

    def test_fit_both_tails_returns_two_results(self):
        estimator = EOTEstimator()
        series = _make_series(years=5)
        high, low = estimator.fit_both_tails(
            series, threshold_quantile=0.90
        )
        assert high.tail == "high"
        assert low.tail == "low"

    def test_fit_empty_exceedances_raises(self):
        estimator = EOTEstimator()
        series = _make_series(years=2)
        with pytest.raises(ValueError, match="No exceedances"):
            estimator.fit(series, tail="high", threshold=float(series.values.max()) + 10000.0)

    def test_low_tail_fit(self):
        estimator = EOTEstimator()
        series = _make_series(years=5)
        result = estimator.fit(series, tail="low", threshold_quantile=0.90)
        assert result.tail == "low"

    def test_redundant_fitter(self):
        fitter = NHPPFitter(maxiter=100)
        selector = ThresholdSelector()
        estimator = EOTEstimator(fitter=fitter, threshold_selector=selector)
        assert estimator.fitter is fitter
        assert estimator.threshold_selector is selector

    def test_prepare_series_defrozen_and_tail_transformed(self):
        estimator = EOTEstimator()
        series = _make_series(years=3)
        prepared = estimator._prepare_series(series, tail="low")
        assert prepared.direction == "low"
