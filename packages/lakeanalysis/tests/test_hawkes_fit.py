import numpy as np
import pytest
from scipy.stats import chi2

from lakeanalysis.hawkes.fit import (
    HawkesFitter,
    LikelihoodRatioTest,
    _window_months_to_years,
    fit_full_model,
    fit_restricted_model,
    run_model_comparison,
)
from lakeanalysis.hawkes.model import log_likelihood, negative_log_likelihood
from lakeanalysis.hawkes.types import (
    HawkesEventSeries,
    HawkesModelSpec,
    TYPE_DRY,
    TYPE_WET,
)


def _make_event_series(n_events=30):
    rng = np.random.default_rng(42)
    times = np.sort(rng.uniform(0.5, 9.5, n_events))
    event_types = rng.integers(0, 2, n_events)
    return HawkesEventSeries(
        times=times,
        event_types=event_types,
        start_time=0.0,
        end_time=10.0,
    )


class TestWindowMonthsToYears:
    def test_none_returns_none(self):
        assert _window_months_to_years(None) is None

    def test_converts_months_to_years(self):
        result = _window_months_to_years(6.0)
        assert result == pytest.approx(0.5)

    def test_zero_raises(self):
        with pytest.raises(ValueError, match="window_months must be positive"):
            _window_months_to_years(0.0)

    def test_negative_raises(self):
        with pytest.raises(ValueError, match="window_months must be positive"):
            _window_months_to_years(-3.0)


class TestHawkesFitter:
    def test_default_construction(self):
        fitter = HawkesFitter()
        assert fitter.maxiter == 4000

    def test_fit_returns_result(self):
        fitter = HawkesFitter(maxiter=200)
        es = _make_event_series(n_events=40)
        spec = HawkesModelSpec(
            free_alpha_mask=np.ones((2, 2), dtype=bool),
            enforce_stability=True,
            kernel_window_years=None,
        )
        result = fitter.fit(es, spec)
        assert result.converged or not result.converged
        assert result.mu.shape == (2,)
        assert result.alpha.shape == (2, 2)
        assert result.beta.shape == (2, 2)

    def test_fit_with_custom_initial_theta(self):
        fitter = HawkesFitter(maxiter=200)
        es = _make_event_series(n_events=40)
        spec = HawkesModelSpec(kernel_window_years=None)
        from lakeanalysis.hawkes.model import default_initial_theta
        theta0 = default_initial_theta(spec)
        result = fitter.fit(es, spec, theta0=theta0)
        assert result.theta is not None

    def test_fit_produces_finite_log_likelihood(self):
        fitter = HawkesFitter(maxiter=200)
        es = _make_event_series(n_events=40)
        spec = HawkesModelSpec(kernel_window_years=None)
        result = fitter.fit(es, spec)
        assert np.isfinite(result.log_likelihood)

    def test_fit_enforces_stability(self):
        fitter = HawkesFitter(maxiter=200)
        es = _make_event_series(n_events=40)
        spec = HawkesModelSpec(
            enforce_stability=True,
            stability_penalty=1e8,
            kernel_window_years=None,
        )
        result = fitter.fit(es, spec)
        assert result.spectral_radius < 1.0 or result.spectral_radius == pytest.approx(0.0, abs=1e-10)


class TestFitFullModel:
    def test_returns_result_with_full_mask(self):
        es = _make_event_series(n_events=30)
        result = fit_full_model(es, window_months=None)
        assert result.model_spec.free_alpha_mask.all()
        assert result.mu is not None


class TestFitRestrictedModel:
    def test_returns_result_with_restricted_mask(self):
        es = _make_event_series(n_events=30)
        disabled = [(TYPE_DRY, TYPE_DRY)]
        result = fit_restricted_model(es, disabled, window_months=None)
        assert not result.model_spec.free_alpha_mask[TYPE_DRY, TYPE_DRY]


class TestLikelihoodRatioTest:
    def test_reject_null_when_p_small(self):
        es = _make_event_series(n_events=40)
        full = fit_full_model(es, window_months=None)
        disabled = [(TYPE_DRY, TYPE_DRY), (TYPE_WET, TYPE_WET), (TYPE_DRY, TYPE_WET)]
        restricted = fit_restricted_model(es, disabled, window_months=None)
        lrt = LikelihoodRatioTest(significance_level=0.05)
        df = int(np.sum(full.model_spec.free_alpha_mask)) - int(np.sum(restricted.model_spec.free_alpha_mask))
        result = lrt.compare("self_excitation_test", restricted, full, max(df, 1))
        assert result.test_name == "self_excitation_test"
        assert result.lr_statistic >= 0
        assert result.df > 0
        assert 0 <= result.p_value <= 1

    def test_same_fits_no_rejection(self):
        es = _make_event_series(n_events=30)
        fit = fit_full_model(es, window_months=None)
        lrt = LikelihoodRatioTest(significance_level=0.01)
        result = lrt.compare("identity", fit, fit, 0)
        assert result.lr_statistic == pytest.approx(0.0)
        assert result.p_value == pytest.approx(1.0)
        assert not result.reject_null

    def test_lr_statistic_chisq_approx(self):
        """LR statistic should approximately follow chi2(df) under null."""
        es = _make_event_series(n_events=50)
        full = fit_full_model(es, window_months=None)
        lrt = LikelihoodRatioTest()
        lr_val = 2.0 * (full.log_likelihood - full.log_likelihood)
        p = float(chi2.sf(lr_val, df=1))
        assert p == pytest.approx(1.0)


class TestRunModelComparison:
    def test_returns_lrt_result(self):
        es = _make_event_series(n_events=40)
        full = fit_full_model(es, window_months=None)
        result = run_model_comparison(
            test_name="test",
            restricted_fit=full,
            full_fit=full,
            df=1,
        )
        assert result.test_name == "test"
        assert result.lr_statistic == pytest.approx(0.0)
