"""Fitting and model-comparison utilities for Hawkes models."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from scipy.optimize import minimize
from scipy.stats import chi2

from . import model as hawkes_model
from .types import (
    HawkesEventSeries,
    HawkesFitResult,
    HawkesModelSpec,
    LRTResult,
    ModelComparisonTest,
)


def _window_months_to_years(window_months: float | None) -> float | None:
    """Convert a month window to year units used by HawkesEventSeries time."""
    if window_months is None:
        return None
    months = float(window_months)
    if months <= 0.0:
        raise ValueError("window_months must be positive when provided")
    return months / 12.0


@dataclass(frozen=True)
class HawkesFitter:
    """Numerical optimizer for a two-type Hawkes process."""

    maxiter: int = 4000

    def fit(
        self,
        event_series: HawkesEventSeries,
        spec: HawkesModelSpec,
        theta0: np.ndarray | None = None,
    ) -> HawkesFitResult:
        """Fit Hawkes parameters under a given model specification."""
        initial = (
            hawkes_model.default_initial_theta(spec)
            if theta0 is None
            else np.asarray(theta0, dtype=float)
        )
        result = minimize(
            hawkes_model.negative_log_likelihood,
            initial,
            args=(event_series, spec),
            method="L-BFGS-B",
            options={"maxiter": self.maxiter},
        )
        params = hawkes_model.unpack_theta(result.x, spec)
        return HawkesFitResult(
            theta=np.asarray(result.x, dtype=float),
            mu=params.mu,
            alpha=params.alpha,
            beta=params.beta,
            log_likelihood=hawkes_model.log_likelihood(result.x, event_series, spec),
            converged=bool(result.success),
            message=str(result.message),
            objective_value=float(result.fun),
            branching_matrix=params.branching_matrix,
            spectral_radius=params.spectral_radius,
            model_spec=spec,
            event_series=event_series,
        )


def fit_full_model(
    event_series: HawkesEventSeries,
    fitter: HawkesFitter | None = None,
    window_months: float | None = 4.0,
) -> HawkesFitResult:
    """Fit the full two-type Hawkes model with all excitation edges enabled."""
    active_fitter = HawkesFitter() if fitter is None else fitter
    spec = HawkesModelSpec(
        free_alpha_mask=np.ones((2, 2), dtype=bool),
        kernel_window_years=_window_months_to_years(window_months),
    )
    return active_fitter.fit(event_series, spec)


def fit_restricted_model(
    event_series: HawkesEventSeries,
    disabled_edges: list[tuple[int, int]],
    fitter: HawkesFitter | None = None,
    window_months: float | None = 4.0,
) -> HawkesFitResult:
    """Fit a restricted model by disabling selected alpha edges."""
    active_fitter = HawkesFitter() if fitter is None else fitter
    mask = np.ones((2, 2), dtype=bool)
    for target_type, source_type in disabled_edges:
        mask[int(target_type), int(source_type)] = False
    spec = HawkesModelSpec(
        free_alpha_mask=mask,
        kernel_window_years=_window_months_to_years(window_months),
    )
    return active_fitter.fit(event_series, spec)


@dataclass(frozen=True)
class LikelihoodRatioTest(ModelComparisonTest):
    """Likelihood-ratio test implementation for nested models."""

    significance_level: float = 0.05

    def compare(
        self,
        test_name: str,
        restricted_fit: HawkesFitResult,
        full_fit: HawkesFitResult,
        df: int,
    ) -> LRTResult:
        """Compare nested fits with a chi-square asymptotic reference."""
        lr_statistic = max(
            2.0 * (full_fit.log_likelihood - restricted_fit.log_likelihood),
            0.0,
        )
        p_value = float(chi2.sf(lr_statistic, df=max(df, 1)))
        return LRTResult(
            test_name=test_name,
            lr_statistic=float(lr_statistic),
            df=int(df),
            p_value=p_value,
            significance_level=float(self.significance_level),
            reject_null=bool(p_value < self.significance_level),
            restricted_log_likelihood=float(restricted_fit.log_likelihood),
            full_log_likelihood=float(full_fit.log_likelihood),
        )


def run_model_comparison(
    test_name: str,
    restricted_fit: HawkesFitResult,
    full_fit: HawkesFitResult,
    df: int,
    test_strategy: ModelComparisonTest | None = None,
) -> LRTResult:
    """Run model comparison using dependency-injected test strategy."""
    strategy = LikelihoodRatioTest() if test_strategy is None else test_strategy
    return strategy.compare(
        test_name=test_name,
        restricted_fit=restricted_fit,
        full_fit=full_fit,
        df=df,
    )

