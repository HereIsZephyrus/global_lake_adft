"""NHPP likelihood evaluation for EOT fits."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.quality.frozen import (
    FrozenPlateauSchedule,
    apply_frozen_plateau,
    build_frozen_plateau_schedule,
)

from .models import LocationModel
from .series import MonthlyTimeSeries


class NHPPLogLikelihood:
    """Negative log-likelihood for the non-homogeneous Poisson point process.

    The integral term of the NHPP likelihood integrates the exceedance rate over the
    full observation period. When a time-varying threshold u(t) is supplied the
    integral uses the full u(t) vector evaluated on a fine grid, so the model
    correctly accounts for seasonal and trend variation in the threshold.
    """

    def __init__(
        self,
        series: MonthlyTimeSeries,
        extremes: pd.DataFrame,
        threshold: float,
        location_model: LocationModel,
        integration_points: int = 512,
        u_grid: np.ndarray | None = None,
        frozen_year_months: tuple[int, ...] = tuple(),
        full_series: MonthlyTimeSeries | None = None,
    ) -> None:
        """Initialise the likelihood.

        Args:
            series: Full monthly series used to define the integration domain.
            extremes: Declustered exceedances (one row per cluster representative).
            threshold: Representative scalar threshold (for diagnostics / initialisiation).
            location_model: Parametric model for mu(t).
            integration_points: Number of quadrature points for the intensity integral.
            u_grid: Pre-computed threshold values on the integration grid. If None,
                    the scalar ``threshold`` is broadcast to a constant array.
        """
        if extremes.empty:
            raise ValueError("At least one declustered exceedance is required")
        self.series = series
        self.extremes = extremes.reset_index(drop=True)
        self.threshold = float(threshold)
        self.location_model = location_model
        self.frozen_year_months = frozen_year_months
        self.full_series = full_series
        self.integration_grid = np.linspace(
            0.0,
            series.duration_years,
            integration_points,
        )
        if u_grid is None:
            self.u_grid = np.full_like(self.integration_grid, self.threshold)
        else:
            u_grid = np.asarray(u_grid, dtype=float)
            if u_grid.shape != self.integration_grid.shape:
                raise ValueError(
                    f"u_grid length {len(u_grid)} must equal integration_points {integration_points}"
                )
            self.u_grid = u_grid
        self.extreme_times = self.extremes["time"].to_numpy(dtype=float)
        self.extreme_values = self.extremes["value"].to_numpy(dtype=float)
        self._frozen_schedule = self._build_frozen_schedule()

    @staticmethod
    def _gumbel_integrand(u_value: np.ndarray, mu_value: np.ndarray, sigma: float) -> np.ndarray:
        exponent = np.clip(-(u_value - mu_value) / sigma, -700.0, 700.0)
        return np.exp(exponent)

    @staticmethod
    def _gumbel_log_density(x_value: np.ndarray, mu_value: np.ndarray, sigma: float) -> np.ndarray:
        exponent = np.clip((x_value - mu_value) / sigma, -700.0, 700.0)
        return -np.log(sigma) - exponent

    def _split_theta(self, theta: np.ndarray) -> tuple[np.ndarray, float, float]:
        theta = np.asarray(theta, dtype=float)
        location_params = theta[: self.location_model.n_params]
        sigma = float(theta[-2])
        xi = float(theta[-1])
        return location_params, sigma, xi

    def _build_frozen_schedule(self) -> FrozenPlateauSchedule | None:
        """Build the frozen plateau schedule for full-domain model evaluation."""
        if not self.frozen_year_months:
            return None
        reference_series = self.full_series if self.full_series is not None else self.series
        start_year = int(reference_series.data["year"].min())
        return build_frozen_plateau_schedule(set(self.frozen_year_months), start_year)

    def __call__(self, theta: np.ndarray) -> float:
        """Return the negative log-likelihood for optimisation."""
        location_params, sigma, xi = self._split_theta(theta)
        if sigma <= 0.0:
            return float("inf")

        mu_grid = self.location_model.evaluate(self.integration_grid, location_params)
        mu_extremes = self.location_model.evaluate(self.extreme_times, location_params)
        if self._frozen_schedule is not None:
            anchor_mu = self.location_model.evaluate(
                self._frozen_schedule.anchor_times,
                location_params,
            )
            mu_grid = apply_frozen_plateau(
                self.integration_grid,
                mu_grid,
                self._frozen_schedule,
                anchor_mu,
            )
            mu_extremes = apply_frozen_plateau(
                self.extreme_times,
                mu_extremes,
                self._frozen_schedule,
                anchor_mu,
            )

        if abs(xi) < 1e-6:
            integral_term = self._gumbel_integrand(self.u_grid, mu_grid, sigma)
            log_density = self._gumbel_log_density(
                self.extreme_values,
                mu_extremes,
                sigma,
            )
        else:
            integral_support = 1.0 + xi * (self.u_grid - mu_grid) / sigma
            density_support = 1.0 + xi * (self.extreme_values - mu_extremes) / sigma
            if np.any(integral_support <= 0.0) or np.any(density_support <= 0.0):
                return float("inf")
            integral_term = integral_support ** (-1.0 / xi)
            log_density = (
                -np.log(sigma)
                + (-1.0 / xi - 1.0) * np.log(density_support)
            )

        total_intensity = float(np.trapezoid(integral_term, self.integration_grid))
        log_likelihood = -total_intensity + float(np.sum(log_density))
        if not np.isfinite(log_likelihood):
            return float("inf")
        return -log_likelihood


__all__ = ["NHPPLogLikelihood"]
