"""Core Hawkes likelihood and intensity utilities."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .types import (
    HawkesEventSeries,
    HawkesFitResult,
    HawkesModelSpec,
    TYPE_DRY,
    TYPE_LABELS,
    TYPE_WET,
)

EPSILON = 1e-10


def _intensity_window_years(spec: HawkesModelSpec) -> float | None:
    """Return validated window size in years, or None for full-history kernels."""
    if spec.kernel_window_years is None:
        return None
    return float(spec.kernel_window_years)


def _softplus(values: np.ndarray) -> np.ndarray:
    """Stable softplus transform."""
    values = np.asarray(values, dtype=float)
    return np.log1p(np.exp(-np.abs(values))) + np.maximum(values, 0.0)


@dataclass(frozen=True)
class HawkesParameterView:
    """Named parameter blocks after unconstrained-to-constrained transform."""

    mu: np.ndarray
    alpha: np.ndarray
    beta: np.ndarray

    @property
    def branching_matrix(self) -> np.ndarray:
        """Expected offspring matrix for exponential kernels."""
        return self.alpha

    @property
    def spectral_radius(self) -> float:
        """Spectral radius of the branching matrix."""
        eigvals = np.linalg.eigvals(self.branching_matrix)
        return float(np.max(np.abs(eigvals)))


def n_parameters(spec: HawkesModelSpec) -> int:
    """Return number of unconstrained free parameters for a model spec."""
    return 2 + int(np.sum(spec.free_alpha_mask)) + 4


def unpack_theta(theta: np.ndarray, spec: HawkesModelSpec) -> HawkesParameterView:
    """Transform unconstrained theta into positive Hawkes parameters."""
    theta = np.asarray(theta, dtype=float)
    expected = n_parameters(spec)
    if len(theta) != expected:
        raise ValueError(f"theta length {len(theta)} does not match expected {expected}")

    index = 0
    mu = _softplus(theta[index : index + 2]) + EPSILON
    index += 2

    alpha = np.zeros((2, 2), dtype=float)
    free_count = int(np.sum(spec.free_alpha_mask))
    alpha_values = _softplus(theta[index : index + free_count]) + EPSILON
    index += free_count
    alpha[spec.free_alpha_mask] = alpha_values

    beta = _softplus(theta[index : index + 4]).reshape(2, 2) + EPSILON
    return HawkesParameterView(mu=mu, alpha=alpha, beta=beta)


def default_initial_theta(spec: HawkesModelSpec) -> np.ndarray:
    """Build a simple unconstrained initial vector."""
    mu0 = np.full(2, 0.2, dtype=float)
    alpha0 = np.full(int(np.sum(spec.free_alpha_mask)), 0.1, dtype=float)
    beta0 = np.full(4, 2.0, dtype=float)
    # Approximate inverse-softplus via log(exp(x)-1)
    positive = np.concatenate([mu0, alpha0, beta0])
    unconstrained = np.log(np.expm1(positive))
    return unconstrained.astype(float)


def event_intensities_at_events(
    event_series: HawkesEventSeries,
    params: HawkesParameterView,
    window_years: float | None = None,
) -> np.ndarray:
    """Evaluate type-specific intensity at each observed event time."""
    times = event_series.times
    event_types = event_series.event_types
    n_events = len(times)
    intensities = np.zeros(n_events, dtype=float)
    if n_events == 0:
        return intensities

    if window_years is not None and window_years <= 0.0:
        raise ValueError("window_years must be positive when provided")

    # Rolling kernel state:
    # state[t, s] = sum(exp(-beta[t, s] * (current_time - tau_j))) over active
    # source-s events tau_j, optionally restricted by a hard support window.
    state = np.zeros((2, 2), dtype=float)
    source_ptr = np.zeros(2, dtype=int)
    active_times: list[list[float]] = [[], []]
    last_time = float(times[0])

    for i in range(n_events):
        current_time = float(times[i])
        dt = current_time - last_time
        if dt < 0.0:
            raise ValueError("event times must be non-decreasing")
        if dt > 0.0:
            state *= np.exp(-params.beta * dt)

        if window_years is not None:
            cutoff = current_time - float(window_years)
            for source in (TYPE_DRY, TYPE_WET):
                ptr = int(source_ptr[source])
                source_times = active_times[source]
                while ptr < len(source_times) and source_times[ptr] < cutoff:
                    expired_time = source_times[ptr]
                    for target in (TYPE_DRY, TYPE_WET):
                        state[target, source] -= float(
                            np.exp(-params.beta[target, source] * (current_time - expired_time))
                        )
                    ptr += 1
                source_ptr[source] = ptr
            state = np.maximum(state, 0.0)

        current_type = int(event_types[i])
        contribution = params.alpha * params.beta * state
        intensities[i] = max(float(params.mu[current_type] + np.sum(contribution[current_type, :])), EPSILON)

        for target in (TYPE_DRY, TYPE_WET):
            state[target, current_type] += 1.0
        active_times[current_type].append(current_time)
        last_time = current_time
    return intensities


def integral_intensity(
    event_series: HawkesEventSeries,
    params: HawkesParameterView,
    window_years: float | None = None,
) -> float:
    """Closed-form integral of total intensity over the observation horizon."""
    horizon = event_series.duration
    baseline = float(np.sum(params.mu) * horizon)
    history_term = 0.0
    for event_time, source_type in zip(event_series.times, event_series.event_types, strict=True):
        tail = float(event_series.end_time - event_time)
        if window_years is not None:
            tail = min(tail, float(window_years))
        for target_type in (TYPE_DRY, TYPE_WET):
            decay = params.beta[target_type, int(source_type)]
            excitation = params.alpha[target_type, int(source_type)]
            history_term += float(excitation * (1.0 - np.exp(-decay * tail)))
    return baseline + history_term


def log_likelihood(
    theta: np.ndarray,
    event_series: HawkesEventSeries,
    spec: HawkesModelSpec,
) -> float:
    """Compute Hawkes log-likelihood under one parameter vector."""
    params = unpack_theta(theta, spec)
    if spec.enforce_stability and params.spectral_radius >= 1.0:
        penalty = spec.stability_penalty * (params.spectral_radius - 1.0 + EPSILON)
        return float(-penalty)
    window_years = _intensity_window_years(spec)
    if len(event_series.times) == 0:
        return -integral_intensity(event_series, params, window_years=window_years)
    intensities = event_intensities_at_events(event_series, params, window_years=window_years)
    integral = integral_intensity(event_series, params, window_years=window_years)
    return float(np.sum(np.log(intensities)) - integral)


def negative_log_likelihood(
    theta: np.ndarray,
    event_series: HawkesEventSeries,
    spec: HawkesModelSpec,
) -> float:
    """Objective function for minimizers."""
    value = -log_likelihood(theta, event_series, spec)
    if not np.isfinite(value):
        return float("inf")
    return float(value)


def evaluate_intensity_decomposition(
    event_series: HawkesEventSeries,
    fit_result: HawkesFitResult,
    evaluation_times: np.ndarray,
    window_years: float | None = None,
) -> pd.DataFrame:
    """Evaluate baseline/self/cross decomposition on a time grid."""
    times = np.asarray(evaluation_times, dtype=float)
    if times.ndim != 1:
        raise ValueError("evaluation_times must be a 1-D array")
    if window_years is None:
        window_years = fit_result.model_spec.kernel_window_years
    if len(times) == 0:
        return pd.DataFrame(columns=["time"])
    if window_years is not None and window_years <= 0.0:
        raise ValueError("window_years must be positive when provided")

    order = np.argsort(times)
    sorted_times = times[order]
    rows_sorted: list[dict[str, float]] = []
    event_times = event_series.times
    event_types = event_series.event_types
    n_events = len(event_times)
    enter_idx = 0
    source_ptr = np.zeros(2, dtype=int)
    active_times: list[list[float]] = [[], []]
    state = np.zeros((2, 2), dtype=float)
    last_time = float(sorted_times[0])

    for t in sorted_times:
        current_t = float(t)
        while enter_idx < n_events and float(event_times[enter_idx]) < current_t:
            event_t = float(event_times[enter_idx])
            dt = event_t - last_time
            if dt < 0.0:
                raise ValueError("event times must be non-decreasing")
            if dt > 0.0:
                state *= np.exp(-fit_result.beta * dt)
            source = int(event_types[enter_idx])
            for target in (TYPE_DRY, TYPE_WET):
                state[target, source] += 1.0
            active_times[source].append(event_t)
            last_time = event_t
            enter_idx += 1

        dt_to_eval = current_t - last_time
        if dt_to_eval < 0.0:
            raise ValueError("evaluation_times must be non-decreasing")
        if dt_to_eval > 0.0:
            state *= np.exp(-fit_result.beta * dt_to_eval)
        last_time = current_t

        if window_years is not None:
            cutoff = current_t - float(window_years)
            for source in (TYPE_DRY, TYPE_WET):
                ptr = int(source_ptr[source])
                source_times = active_times[source]
                while ptr < len(source_times) and source_times[ptr] < cutoff:
                    expired_time = source_times[ptr]
                    for target in (TYPE_DRY, TYPE_WET):
                        state[target, source] -= float(
                            np.exp(-fit_result.beta[target, source] * (current_t - expired_time))
                        )
                    ptr += 1
                source_ptr[source] = ptr
            state = np.maximum(state, 0.0)

        row: dict[str, float] = {"time": current_t}
        components = fit_result.alpha * fit_result.beta * state
        for target_type, label in zip((TYPE_DRY, TYPE_WET), TYPE_LABELS, strict=True):
            baseline = float(fit_result.mu[target_type])
            self_component = float(components[target_type, target_type])
            cross_component = float(components[target_type, 1 - target_type])
            row[f"mu_{label}"] = baseline
            row[f"self_{label}"] = self_component
            row[f"cross_{label}"] = cross_component
            row[f"lambda_{label}"] = baseline + self_component + cross_component
        rows_sorted.append(row)

    rows: list[dict[str, float]] = [rows_sorted[idx] for idx in np.argsort(order)]
    return pd.DataFrame(rows)

