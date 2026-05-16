"""Shared EOT model dataclasses and parametric components."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from lakeanalysis.quality.frozen import (
    FrozenPlateauSchedule,
    apply_frozen_plateau,
    build_frozen_plateau_schedule,
)

from .basis import BaseBasis, HarmonicBasis
from .preprocess import QuantileThresholdModel
from .series import MonthlyTimeSeries, TailDirection


@dataclass(frozen=True)
class LocationModel:
    """Parametric location model for a seasonal non-stationary NHPP."""

    include_trend: bool = True
    n_harmonics: int = 1
    basis_model: BaseBasis | None = None

    def __post_init__(self) -> None:
        """Initialise the injected basis model while preserving legacy arguments."""
        if self.n_harmonics < 1:
            raise ValueError("n_harmonics must be >= 1")
        if self.basis_model is None:
            object.__setattr__(self, "basis_model", HarmonicBasis(self.n_harmonics))

    @property
    def param_names(self) -> tuple[str, ...]:
        """Return the ordered parameter names of the location model."""
        names = ["beta0"]
        if self.include_trend:
            names.append("beta1")
        if self.basis_model is None:
            raise ValueError("basis_model must be initialised before requesting parameter names")
        names.extend(self.basis_model.parameter_names)
        return tuple(names)

    @property
    def n_params(self) -> int:
        """Return the number of free parameters in the location model."""
        return len(self.param_names)

    def design_matrix(self, times: np.ndarray) -> np.ndarray:
        """Build the design matrix associated with the time points."""
        if self.basis_model is None:
            raise ValueError("basis_model must be initialised before building the design matrix")
        return self.basis_model.build_design_matrix(
            np.asarray(times, dtype=float),
            include_trend=self.include_trend,
            include_intercept=True,
        )

    def evaluate(
        self,
        times: np.ndarray,
        params: np.ndarray,
    ) -> np.ndarray:
        """Evaluate the location function mu(t)."""
        return self.design_matrix(times) @ np.asarray(params, dtype=float)


@dataclass(frozen=True)
class FitResult:
    """Container for NHPP fit results."""

    theta: np.ndarray
    covariance: np.ndarray
    threshold: float
    tail: TailDirection
    log_likelihood: float
    converged: bool
    message: str
    location_model: LocationModel
    series: MonthlyTimeSeries
    extremes: pd.DataFrame
    threshold_model: QuantileThresholdModel | None = None
    threshold_params: np.ndarray | None = None
    full_series: MonthlyTimeSeries | None = None
    frozen_year_months: tuple[int, ...] = tuple()

    @property
    def param_names(self) -> tuple[str, ...]:
        """Return the ordered free-parameter names."""
        return self.location_model.param_names + ("sigma", "xi")

    @property
    def params(self) -> dict[str, float]:
        """Return parameters as a name-to-value dictionary."""
        return {
            name: float(value)
            for name, value in zip(self.param_names, self.theta, strict=True)
        }

    @property
    def sigma(self) -> float:
        """Return the scale parameter."""
        return float(self.theta[-2])

    @property
    def xi(self) -> float:
        """Return the shape parameter."""
        return float(self.theta[-1])

    def mu(self, times: np.ndarray) -> np.ndarray:
        """Evaluate mu(t) using the fitted location parameters."""
        times = np.asarray(times, dtype=float)
        mu_values = self.location_model.evaluate(times, self.theta[: self.location_model.n_params])
        schedule = self._frozen_plateau_schedule()
        if schedule is None:
            return mu_values
        anchor_values = self.location_model.evaluate(
            schedule.anchor_times,
            self.theta[: self.location_model.n_params],
        )
        return apply_frozen_plateau(times, mu_values, schedule, anchor_values)

    def threshold_at(self, times: np.ndarray) -> np.ndarray:
        """Evaluate the threshold u(t) at arbitrary time points.

        Returns a constant array equal to ``self.threshold`` when no time-varying
        model is stored (i.e. ``threshold_params`` is None).
        """
        times = np.asarray(times, dtype=float)
        if self.threshold_model is not None and self.threshold_params is not None:
            threshold_values = self.threshold_model.evaluate(times, self.threshold_params)
            schedule = self._frozen_plateau_schedule()
            if schedule is None:
                return threshold_values
            anchor_values = self.threshold_model.evaluate(schedule.anchor_times, self.threshold_params)
            return apply_frozen_plateau(times, threshold_values, schedule, anchor_values)
        return np.full_like(times, self.threshold)

    def _frozen_plateau_schedule(self) -> FrozenPlateauSchedule | None:
        """Return the plateau schedule for frozen periods, if available."""
        if not self.frozen_year_months:
            return None
        reference_series = self.full_series if self.full_series is not None else self.series
        start_year = int(reference_series.data["year"].min())
        return build_frozen_plateau_schedule(set(self.frozen_year_months), start_year)

    def with_theta(self, theta: np.ndarray) -> "FitResult":
        """Return a shallow copy with a different parameter vector."""
        return FitResult(
            theta=np.asarray(theta, dtype=float),
            covariance=self.covariance,
            threshold=self.threshold,
            tail=self.tail,
            log_likelihood=self.log_likelihood,
            converged=self.converged,
            message=self.message,
            location_model=self.location_model,
            series=self.series,
            extremes=self.extremes,
            threshold_model=self.threshold_model,
            threshold_params=self.threshold_params,
            full_series=self.full_series,
            frozen_year_months=self.frozen_year_months,
        )


@dataclass(frozen=True)
class PreparedExtremes:
    """Container for pre-fitted threshold and declustered exceedances."""

    series: MonthlyTimeSeries
    full_series: MonthlyTimeSeries
    representative_threshold: float
    extremes: pd.DataFrame
    basis_model: BaseBasis
    threshold_model: QuantileThresholdModel | None
    threshold_params: np.ndarray | None
    u_grid: np.ndarray | None


__all__ = ["LocationModel", "FitResult", "PreparedExtremes"]
