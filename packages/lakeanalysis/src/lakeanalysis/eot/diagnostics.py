"""Diagnostics and return-level estimation for EOT NHPP fits."""

from __future__ import annotations

from dataclasses import dataclass
import logging

import numpy as np
import pandas as pd
from scipy.optimize import brentq

from .estimation import FitResult

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReturnLevelEstimator:
    """Estimate return levels from a fitted non-stationary NHPP model."""

    fit_result: FitResult
    integration_points: int = 512

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_grid",
            np.linspace(0.0, self.fit_result.series.duration_years, self.integration_points),
        )

    def _intensity_integral(self, theta: np.ndarray, z_value: float) -> float:
        location_param_count = self.fit_result.location_model.n_params
        mu_grid = self.fit_result.location_model.evaluate(
            self._grid,
            theta[:location_param_count],
        )
        sigma = float(theta[-2])
        xi = float(theta[-1])
        if sigma <= 0.0:
            return float("inf")
        if abs(xi) < 1e-6:
            integrand = np.exp(-(z_value - mu_grid) / sigma)
        else:
            support = 1.0 + xi * (z_value - mu_grid) / sigma
            if xi > 0.0:
                if np.any(support <= 0.0):
                    return float("inf")
                integrand = support ** (-1.0 / xi)
            else:
                # For xi < 0, values above the local upper endpoint have zero
                # exceedance intensity rather than making the whole integral invalid.
                integrand = np.zeros_like(support, dtype=float)
                positive = support > 0.0
                integrand[positive] = support[positive] ** (-1.0 / xi)
        return float(np.trapezoid(integrand, self._grid))

    def _root_function(
        self,
        theta: np.ndarray,
        z_value: float,
        return_period_years: float,
    ) -> float:
        T_obs = self.fit_result.series.duration_years
        return (return_period_years / T_obs) * self._intensity_integral(theta, z_value) - 1.0

    def _solve_return_level(self, theta: np.ndarray, return_period_years: float) -> float:
        sigma = float(theta[-2])
        xi = float(theta[-1])
        mu_grid = self.fit_result.location_model.evaluate(
            self._grid,
            theta[: self.fit_result.location_model.n_params],
        )
        threshold = self.fit_result.threshold
        step = max(sigma, 1.0)

        # The root function f(z) = (T/T_obs) * integral(z) - 1 is monotone decreasing in z.
        # T_obs is the observation duration in years; the integral gives total intensity
        # over [0, T_obs], so dividing by T_obs yields the annual rate.
        lower_bound = -np.inf
        upper_bound = np.inf
        if xi > 0.0:
            lower_bound = float(np.max(mu_grid) - sigma / xi) + 1e-6
        elif xi < 0.0:
            # With xi < 0, each time point has its own finite upper endpoint.
            # The intensity is zero above a local endpoint, so the global upper
            # limit is the maximum endpoint across time, not the minimum.
            upper_bound = float(np.max(mu_grid) - sigma / xi) - 1e-6

        lower = float(threshold)
        if np.isfinite(lower_bound):
            lower = max(lower, lower_bound)
        if np.isfinite(upper_bound) and lower >= upper_bound:
            lower = upper_bound - step

        root_at_lower = self._root_function(theta, lower, return_period_years)
        if not np.isfinite(root_at_lower):
            raise ValueError("Invalid lower bracket for return level estimation")

        # Move lower left until f(lower) > 0.
        lower_adjustments = 0
        while root_at_lower <= 0.0 and lower_adjustments < 100:
            candidate = lower - step
            if np.isfinite(lower_bound):
                candidate = max(candidate, lower_bound)
            if candidate >= lower:
                break
            lower = candidate
            root_at_lower = self._root_function(theta, lower, return_period_years)
            if not np.isfinite(root_at_lower):
                raise ValueError("Invalid lower bracket for return level estimation")
            lower_adjustments += 1

        if root_at_lower <= 0.0:
            raise ValueError("Could not bracket the return-level root (lower)")

        upper = max(lower + step, threshold + step)
        if np.isfinite(upper_bound):
            upper = min(upper, upper_bound)

        root_at_upper = self._root_function(theta, upper, return_period_years)
        if not np.isfinite(root_at_upper) and np.isfinite(upper_bound):
            upper = upper_bound
            root_at_upper = self._root_function(theta, upper, return_period_years)

        # Move upper right until f(upper) < 0.
        upper_adjustments = 0
        while np.isfinite(root_at_upper) and root_at_upper >= 0.0 and upper_adjustments < 100:
            candidate = upper + step
            if np.isfinite(upper_bound):
                candidate = min(candidate, upper_bound)
            if candidate <= upper:
                break
            upper = candidate
            root_at_upper = self._root_function(theta, upper, return_period_years)
            upper_adjustments += 1

        if not np.isfinite(root_at_upper) and np.isfinite(upper_bound):
            upper = upper_bound
            root_at_upper = self._root_function(theta, upper, return_period_years)

        if (
            lower >= upper
            or not np.isfinite(root_at_lower)
            or not np.isfinite(root_at_upper)
            or root_at_lower <= 0.0
            or root_at_upper >= 0.0
        ):
            raise ValueError(
                "Could not bracket the return-level root (f(lower) must be > 0 and f(upper) < 0)"
            )

        return float(
            brentq(
                lambda z_value: self._root_function(theta, z_value, return_period_years),
                lower,
                upper,
            )
        )

    def _gradient(
        self,
        return_period_years: float,
        base_return_level: float,
    ) -> np.ndarray:
        theta = self.fit_result.theta
        gradient = np.full_like(theta, np.nan, dtype=float)
        for index, value in enumerate(theta):
            step = max(abs(value) * 1e-4, 1e-5)
            forward = theta.copy()
            backward = theta.copy()
            forward[index] += step
            backward[index] -= step
            if index == len(theta) - 2 and backward[index] <= 0.0:
                backward[index] = max(theta[index] * 0.5, 1e-6)
            try:
                z_forward = self._solve_return_level(forward, return_period_years)
                z_backward = self._solve_return_level(backward, return_period_years)
                gradient[index] = (z_forward - z_backward) / (forward[index] - backward[index])
            except ValueError:
                gradient[index] = np.nan
                log.debug(
                    "Failed to compute return-level gradient for parameter %d around %.6f",
                    index,
                    base_return_level,
                )
        return gradient

    def estimate_one(self, return_period_years: float) -> dict[str, float]:
        """Estimate a single return level and its normal-approximation interval."""
        theta = self.fit_result.theta
        z_transformed = self._solve_return_level(theta, return_period_years)
        gradient = self._gradient(return_period_years, z_transformed)
        covariance = self.fit_result.covariance

        variance = float("nan")
        standard_error = float("nan")
        lower_ci = float("nan")
        upper_ci = float("nan")
        if (
            covariance.shape == (len(theta), len(theta))
            and np.all(np.isfinite(covariance))
            and np.all(np.isfinite(gradient))
        ):
            variance = float(gradient @ covariance @ gradient)
            if variance >= 0.0:
                standard_error = float(np.sqrt(variance))
                lower_ci = z_transformed - 1.96 * standard_error
                upper_ci = z_transformed + 1.96 * standard_error

        if self.fit_result.tail == "low":
            return_level = -z_transformed
            ci_lower = -upper_ci if np.isfinite(upper_ci) else float("nan")
            ci_upper = -lower_ci if np.isfinite(lower_ci) else float("nan")
        else:
            return_level = z_transformed
            ci_lower = lower_ci
            ci_upper = upper_ci

        return {
            "return_period_years": float(return_period_years),
            "return_level": float(return_level),
            "standard_error": standard_error,
            "ci_lower": float(ci_lower) if np.isfinite(ci_lower) else float("nan"),
            "ci_upper": float(ci_upper) if np.isfinite(ci_upper) else float("nan"),
        }

    def estimate(
        self,
        return_periods: list[float] | tuple[float, ...] = (10.0, 25.0, 50.0, 100.0),
    ) -> pd.DataFrame:
        """Estimate multiple return levels."""
        records = [self.estimate_one(period) for period in return_periods]
        return pd.DataFrame(records)


@dataclass(frozen=True)
class ModelChecker:
    """Build residual-based diagnostics for a fitted NHPP model."""

    fit_result: FitResult

    def transformed_residuals(self) -> np.ndarray:
        """Return exponential residuals implied by the fitted non-stationary GPD.

        Uses the threshold at each exceedance time (u(t_j)) so that the residual
        transform is valid for both fixed and time-varying thresholds. The extremes
        DataFrame has a "threshold" column with the threshold at each point.
        """
        extremes = self.fit_result.extremes
        times = extremes["time"].to_numpy(dtype=float)
        values = extremes["value"].to_numpy(dtype=float)
        u_t = extremes["threshold"].to_numpy(dtype=float)
        mu_t = self.fit_result.mu(times)
        sigma = self.fit_result.sigma
        xi = self.fit_result.xi
        excess = values - u_t

        if abs(xi) < 1e-6:
            residuals = excess / sigma
        else:
            sigma_u = sigma + xi * (u_t - mu_t)
            support = 1.0 + xi * excess / sigma_u
            if np.any(support <= 0.0) or np.any(sigma_u <= 0.0):
                raise ValueError("Residual transform is invalid under the fitted parameters")
            residuals = np.log(support) / xi

        return np.sort(np.asarray(residuals, dtype=float))

    def probability_plot_data(self) -> pd.DataFrame:
        """Return data for a probability plot against the unit-exponential law."""
        residuals = self.transformed_residuals()
        n_obs = residuals.size
        empirical = np.arange(1, n_obs + 1, dtype=float) / (n_obs + 1.0)
        model = 1.0 - np.exp(-residuals)
        return pd.DataFrame(
            {
                "empirical_probability": empirical,
                "model_probability": model,
            }
        )

    def quantile_plot_data(self) -> pd.DataFrame:
        """Return data for a quantile plot against the unit-exponential law."""
        residuals = self.transformed_residuals()
        n_obs = residuals.size
        theoretical = -np.log(1.0 - np.arange(1, n_obs + 1, dtype=float) / (n_obs + 1.0))
        return pd.DataFrame(
            {
                "theoretical_quantile": theoretical,
                "empirical_quantile": residuals,
            }
        )
