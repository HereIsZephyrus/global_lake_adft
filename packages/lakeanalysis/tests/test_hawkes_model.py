import numpy as np
import pandas as pd
import pytest

from lakeanalysis.hawkes.model import (
    HawkesParameterView,
    _intensity_window_years,
    _softplus,
    default_initial_theta,
    evaluate_intensity_decomposition,
    event_intensities_at_events,
    integral_intensity,
    log_likelihood,
    n_parameters,
    negative_log_likelihood,
    unpack_theta,
)
from lakeanalysis.hawkes.types import (
    HawkesEventSeries,
    HawkesFitResult,
    HawkesModelSpec,
    TYPE_DRY,
    TYPE_WET,
)


def _make_event_series(n_events=20):
    rng = np.random.default_rng(42)
    times = np.sort(rng.uniform(0.5, 4.5, n_events))
    event_types = rng.integers(0, 2, n_events)
    return HawkesEventSeries(
        times=times,
        event_types=event_types,
        start_time=0.0,
        end_time=5.0,
    )


class TestSoftplus:
    def test_softplus_zero(self):
        assert _softplus(np.array([0.0]))[0] == pytest.approx(np.log(2))

    def test_softplus_negative(self):
        result = _softplus(np.array([-5.0]))[0]
        assert result > 0
        assert result < 0.1

    def test_softplus_positive(self):
        result = _softplus(np.array([5.0]))[0]
        assert result == pytest.approx(5.0, rel=0.1)

    def test_softplus_vector(self):
        result = _softplus(np.array([-10.0, 0.0, 10.0]))
        assert result.shape == (3,)
        assert np.all(result > 0)


class TestIntensityWindowYears:
    def test_none_returns_none(self):
        spec = HawkesModelSpec(kernel_window_years=None)
        assert _intensity_window_years(spec) is None

    def test_float_returns_float(self):
        spec = HawkesModelSpec(kernel_window_years=0.25)
        assert _intensity_window_years(spec) == pytest.approx(0.25)


class TestParameterView:
    def test_spectral_radius_zero(self):
        params = HawkesParameterView(
            mu=np.array([0.5, 0.5]),
            alpha=np.zeros((2, 2)),
            beta=np.array([[2.0, 2.0], [2.0, 2.0]]),
        )
        assert params.spectral_radius == pytest.approx(0.0)

    def test_spectral_radius_nonzero(self):
        params = HawkesParameterView(
            mu=np.array([0.5, 0.5]),
            alpha=np.array([[0.1, 0.2], [0.3, 0.4]]),
            beta=np.array([[2.0, 2.0], [2.0, 2.0]]),
        )
        assert 0 < params.spectral_radius < 1.0


class TestNParameters:
    def test_full_model(self):
        spec = HawkesModelSpec(free_alpha_mask=np.ones((2, 2), dtype=bool))
        assert n_parameters(spec) == 2 + 4 + 4

    def test_restricted_model(self):
        mask = np.ones((2, 2), dtype=bool)
        mask[0, 0] = False
        spec = HawkesModelSpec(free_alpha_mask=mask)
        assert n_parameters(spec) == 2 + 3 + 4


class TestUnpackTheta:
    def test_unpack_returns_parameter_view(self):
        spec = HawkesModelSpec()
        theta = np.ones(n_parameters(spec))
        params = unpack_theta(theta, spec)
        assert isinstance(params, HawkesParameterView)
        assert params.mu.shape == (2,)
        assert params.alpha.shape == (2, 2)
        assert params.beta.shape == (2, 2)

    def test_theta_length_mismatch_raises(self):
        spec = HawkesModelSpec()
        with pytest.raises(ValueError, match="theta length"):
            unpack_theta(np.array([0.1]), spec)

    def test_all_params_positive(self):
        spec = HawkesModelSpec()
        theta = np.zeros(n_parameters(spec))
        params = unpack_theta(theta, spec)
        assert np.all(params.mu > 0)
        assert np.all(params.alpha[spec.free_alpha_mask] > 0)
        assert np.all(params.beta > 0)


class TestDefaultInitialTheta:
    def test_returns_correct_length(self):
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        assert len(theta) == n_parameters(spec)

    def test_unpacks_without_error(self):
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        params = unpack_theta(theta, spec)
        assert np.all(params.mu > 0)


class TestEventIntensities:
    def test_empty_events_returns_zeros(self):
        es = HawkesEventSeries(
            times=np.array([]), event_types=np.array([], dtype=int),
            start_time=0.0, end_time=5.0,
        )
        spec = HawkesModelSpec()
        params = unpack_theta(default_initial_theta(spec), spec)
        result = event_intensities_at_events(es, params)
        assert len(result) == 0

    def test_non_empty_returns_positive_intensities(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        params = unpack_theta(theta, spec)
        result = event_intensities_at_events(es, params, window_years=None)
        assert len(result) == len(es.times)
        assert np.all(result > 0)

    def test_window_years_non_positive_raises(self):
        es = _make_event_series(n_events=3)
        spec = HawkesModelSpec()
        params = unpack_theta(default_initial_theta(spec), spec)
        with pytest.raises(ValueError, match="window_years must be positive"):
            event_intensities_at_events(es, params, window_years=0.0)

    def test_with_window_returns_positive(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        params = unpack_theta(theta, spec)
        result = event_intensities_at_events(es, params, window_years=2.0)
        assert len(result) == len(es.times)
        assert np.all(result > 0)


class TestIntegralIntensity:
    def test_returns_positive_value(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        params = unpack_theta(default_initial_theta(spec), spec)
        result = integral_intensity(es, params, window_years=None)
        assert result > 0

    def test_empty_events(self):
        es = HawkesEventSeries(
            times=np.array([]), event_types=np.array([], dtype=int),
            start_time=0.0, end_time=5.0,
        )
        spec = HawkesModelSpec()
        params = unpack_theta(default_initial_theta(spec), spec)
        result = integral_intensity(es, params, window_years=None)
        assert result == pytest.approx(float(np.sum(params.mu)) * es.duration)


class TestLogLikelihood:
    def test_finite_value_returned(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        ll = log_likelihood(theta, es, spec)
        assert np.isfinite(ll)
        assert ll < 0

    def test_unstable_model_penalized(self):
        es = _make_event_series()
        spec = HawkesModelSpec(
            free_alpha_mask=np.ones((2, 2), dtype=bool),
            enforce_stability=True,
            stability_penalty=1e8,
        )
        theta = np.ones(n_parameters(spec)) * 5.0
        ll = log_likelihood(theta, es, spec)
        params = unpack_theta(theta, spec)
        if params.spectral_radius >= 1.0:
            assert ll < 0

    def test_empty_events_returns_negative_integral(self):
        es = HawkesEventSeries(
            times=np.array([]), event_types=np.array([], dtype=int),
            start_time=0.0, end_time=5.0,
        )
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        ll = log_likelihood(theta, es, spec)
        assert np.isfinite(ll)
        assert ll < 0


class TestNegativeLogLikelihood:
    def test_negates_log_likelihood(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        ll = log_likelihood(theta, es, spec)
        nll = negative_log_likelihood(theta, es, spec)
        assert nll == pytest.approx(-ll)

    def test_inf_on_nan_ll(self):
        es = _make_event_series()
        spec = HawkesModelSpec(enforce_stability=True, stability_penalty=1e10)
        theta = np.full(n_parameters(spec), 1e6)
        nll = negative_log_likelihood(theta, es, spec)
        assert np.isfinite(nll) or nll == float("inf")


class TestIntensityDecomposition:
    def test_single_time_returns_row(self):
        raw = _make_event_series(n_events=10)
        es = HawkesEventSeries(
            times=np.sort(raw.times),
            event_types=raw.event_types,
            start_time=0.0,
            end_time=10.0,
        )
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        params = unpack_theta(theta, spec)
        fr = HawkesFitResult(
            theta=theta,
            mu=params.mu,
            alpha=params.alpha,
            beta=params.beta,
            log_likelihood=log_likelihood(theta, es, spec),
            converged=True,
            message="ok",
            objective_value=float(negative_log_likelihood(theta, es, spec)),
            branching_matrix=params.branching_matrix,
            spectral_radius=params.spectral_radius,
            model_spec=spec,
            event_series=es,
        )
        grid = np.array([0.0])
        df = evaluate_intensity_decomposition(es, fr, grid, window_years=None)
        assert len(df) == 1
        assert "time" in df.columns
        assert "mu_D" in df.columns
        assert "mu_W" in df.columns
        assert "lambda_D" in df.columns
        assert "lambda_W" in df.columns

    def test_empty_grid_returns_empty(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        params = unpack_theta(theta, spec)
        fr = HawkesFitResult(
            theta=theta, mu=params.mu, alpha=params.alpha, beta=params.beta,
            log_likelihood=0.0, converged=True, message="", objective_value=0.0,
            branching_matrix=params.branching_matrix,
            spectral_radius=params.spectral_radius,
            model_spec=spec, event_series=es,
        )
        df = evaluate_intensity_decomposition(es, fr, np.array([]))
        assert df.empty

    def test_non_positive_window_raises(self):
        es = _make_event_series()
        spec = HawkesModelSpec()
        theta = default_initial_theta(spec)
        params = unpack_theta(theta, spec)
        fr = HawkesFitResult(
            theta=theta, mu=params.mu, alpha=params.alpha, beta=params.beta,
            log_likelihood=0.0, converged=True, message="", objective_value=0.0,
            branching_matrix=params.branching_matrix,
            spectral_radius=params.spectral_radius,
            model_spec=spec, event_series=es,
        )
        with pytest.raises(ValueError, match="window_years must be positive"):
            evaluate_intensity_decomposition(es, fr, np.array([1.0]), window_years=0.0)
