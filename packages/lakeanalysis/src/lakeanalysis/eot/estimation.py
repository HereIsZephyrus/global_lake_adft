"""NHPP estimation orchestrators for excess-over-threshold monthly extremes."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import warnings

import numpy as np
import pandas as pd
from scipy.optimize import minimize

from lakeanalysis.extreme.evt import fit_gpd_exceedances
from .basis import BaseBasis, BasisSelector, HarmonicBasis
from .likelihood import NHPPLogLikelihood
from .models import FitResult, LocationModel, PreparedExtremes
from .preprocess import (
    DeclusteringStrategy,
    QuantileThresholdModel,
    ThresholdSelector,
    RunsDeclustering,
)
from .series import MIN_OBSERVATIONS, MonthlyTimeSeries, TailDirection
from lakeanalysis.quality.frozen import apply_frozen_plateau, build_frozen_plateau_schedule

log = logging.getLogger(__name__)

EPSILON = 1e-8


@dataclass(frozen=True)
class NHPPFitter:
    """Fit a seasonal NHPP model to declustered threshold exceedances."""

    location_model: LocationModel = LocationModel()
    maxiter: int = 2000
    integration_points: int = 256
    max_restarts: int = 4
    enable_powell_fallback: bool = True

    def _initial_location_params(self, series: MonthlyTimeSeries) -> np.ndarray:
        """Initialise mu(t) by least squares on the full transformed series."""
        design = self.location_model.design_matrix(series.data["time"].to_numpy(dtype=float))
        response = series.data["value"].to_numpy(dtype=float)
        coefficients, *_ = np.linalg.lstsq(design, response, rcond=None)
        return coefficients.astype(float)

    @staticmethod
    def _initial_scale_shape(extremes: pd.DataFrame, threshold: float) -> tuple[float, float]:
        """Initialise sigma and xi from a stationary GPD fit."""
        excesses = extremes["value"].to_numpy(dtype=float) - float(threshold)
        excesses = excesses[excesses >= 0.0]
        if excesses.size >= 3 and (excesses.max() - excesses.min()) > 0.0:
            try:
                shape, scale = fit_gpd_exceedances(excesses)
                sigma = max(scale, EPSILON)
                xi = float(np.clip(shape, -0.9, 0.9))
                return sigma, xi
            except (RuntimeError, ValueError):
                log.debug("Falling back to moment-based initial values for sigma/xi")

        sigma = max(float(np.std(excesses, ddof=1)) if excesses.size > 1 else 1.0, EPSILON)
        return sigma, 0.1

    def initial_theta(
        self,
        series: MonthlyTimeSeries,
        extremes: pd.DataFrame,
        threshold: float,
    ) -> np.ndarray:
        """Build the default optimisation starting point."""
        location_params = self._initial_location_params(series)
        sigma, xi = self._initial_scale_shape(extremes, threshold)
        return np.concatenate([location_params, np.array([sigma, xi], dtype=float)])

    def candidate_initial_thetas(
        self,
        series: MonthlyTimeSeries,
        extremes: pd.DataFrame,
        threshold: float,
    ) -> list[np.ndarray]:
        """Return a small set of robust optimisation starting points."""
        base = self.initial_theta(series, extremes, threshold)
        sigma = float(base[-2])
        xi = float(base[-1])
        candidates = [base]
        for xi_candidate in (xi, 0.0, -0.1, 0.1):
            for sigma_scale in (1.0, 0.5, 2.0):
                candidate = base.copy()
                candidate[-2] = max(sigma * sigma_scale, EPSILON)
                candidate[-1] = float(np.clip(xi_candidate, -0.9, 0.9))
                candidates.append(candidate)

        unique_candidates: list[np.ndarray] = []
        seen: set[tuple[float, ...]] = set()
        for candidate in candidates:
            key = tuple(np.round(candidate, 8))
            if key in seen:
                continue
            seen.add(key)
            unique_candidates.append(candidate)
        return unique_candidates

    def fit(
        self,
        series: MonthlyTimeSeries,
        extremes: pd.DataFrame,
        threshold: float,
        tail: TailDirection,
        u_grid: np.ndarray | None = None,
        threshold_model: QuantileThresholdModel | None = None,
        threshold_params: np.ndarray | None = None,
        full_series: MonthlyTimeSeries | None = None,
        frozen_year_months: tuple[int, ...] = tuple(),
    ) -> FitResult:
        """Fit the NHPP model and return a structured result object.

        Args:
            series: Full monthly series (working-tail scale).
            extremes: Declustered exceedances from ``DeclusteringStrategy.decluster``.
            threshold: Representative scalar threshold (used for initialisiation and
                       stored in ``FitResult.threshold`` for reference).
            tail: Tail direction.
            u_grid: Threshold values on the integration grid (``integration_points``
                    elements).  When supplied, the NHPP integral uses the time-varying
                    u(t) instead of the constant scalar.  Pass None to fall back to the
                    constant-threshold behaviour.
            threshold_model: The ``QuantileThresholdModel`` instance used to generate
                             ``threshold_params``; stored in ``FitResult`` for later
                             evaluation.
            threshold_params: Fitted parameter vector of the ``QuantileThresholdModel``;
                              stored in ``FitResult`` so u(t) can be reconstructed
                              anywhere a ``FitResult`` is available.
        """
        objective = NHPPLogLikelihood(
            series=series,
            extremes=extremes,
            threshold=threshold,
            location_model=self.location_model,
            integration_points=self.integration_points,
            u_grid=u_grid,
            frozen_year_months=frozen_year_months,
            full_series=full_series,
        )
        candidate_thetas = self.candidate_initial_thetas(series, extremes, threshold)
        if self.max_restarts < 1:
            raise ValueError("max_restarts must be >= 1")
        candidate_thetas = candidate_thetas[: self.max_restarts]
        bounds = [(None, None)] * self.location_model.n_params + [
            (EPSILON, None),
            (-0.95, 0.95),
        ]
        results = []
        for theta0 in candidate_thetas:
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    category=RuntimeWarning,
                    module=r"scipy\.optimize",
                )
                result = minimize(
                    objective,
                    theta0,
                    method="L-BFGS-B",
                    bounds=bounds,
                    options={"maxiter": self.maxiter},
                )
            results.append(result)
            if result.success and np.isfinite(result.fun):
                continue
            if self.enable_powell_fallback:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        category=RuntimeWarning,
                        module=r"scipy\.optimize",
                    )
                    fallback_result = minimize(
                        objective,
                        theta0,
                        method="Powell",
                        bounds=bounds,
                        options={"maxiter": self.maxiter},
                    )
                results.append(fallback_result)

        successful = [result for result in results if result.success and np.isfinite(result.fun)]
        finite = [result for result in results if np.isfinite(result.fun)]
        if successful:
            result = min(successful, key=lambda item: float(item.fun))
        elif finite:
            result = min(finite, key=lambda item: float(item.fun))
        else:
            result = results[0]

        covariance = np.full((len(candidate_thetas[0]), len(candidate_thetas[0])), np.nan, dtype=float)
        if hasattr(result, "hess_inv") and result.hess_inv is not None:
            try:
                covariance = np.asarray(result.hess_inv.todense(), dtype=float)
            except AttributeError:
                hess_inv = np.asarray(result.hess_inv, dtype=float)
                if hess_inv.ndim == 2:
                    covariance = hess_inv

        log_likelihood = -float(result.fun) if np.isfinite(result.fun) else float("nan")
        return FitResult(
            theta=np.asarray(result.x, dtype=float),
            covariance=covariance,
            threshold=float(threshold),
            tail=tail,
            log_likelihood=log_likelihood,
            converged=bool(result.success),
            message=str(result.message),
            location_model=self.location_model,
            series=series,
            extremes=extremes.reset_index(drop=True),
            threshold_model=threshold_model,
            threshold_params=threshold_params,
            full_series=full_series,
            frozen_year_months=frozen_year_months,
        )
@dataclass
class EOTEstimator:
    """High-level estimator orchestrating thresholding, declustering and NHPP fit."""

    threshold_selector: ThresholdSelector | None = None
    declustering_strategy: DeclusteringStrategy | None = None
    fitter: NHPPFitter | None = None
    basis_selector: BasisSelector | None = None
    min_observations: int = MIN_OBSERVATIONS

    def __post_init__(self) -> None:
        if self.threshold_selector is None:
            self.threshold_selector = ThresholdSelector()
        if self.declustering_strategy is None:
            self.declustering_strategy = RunsDeclustering()
        if self.fitter is None:
            self.fitter = NHPPFitter()

    def _coerce_series(self, data: pd.DataFrame | MonthlyTimeSeries) -> MonthlyTimeSeries:
        if isinstance(data, MonthlyTimeSeries):
            return data
        return MonthlyTimeSeries.from_frame(data)

    def _select_basis_model(self, series: MonthlyTimeSeries) -> BaseBasis:
        """Select a basis model or fallback to the fitter template basis."""
        if self.basis_selector is None:
            if self.fitter is None:
                raise ValueError("fitter must be available before selecting a basis model")
            if self.fitter.location_model.basis_model is None:
                return HarmonicBasis(self.fitter.location_model.n_harmonics)
            return self.fitter.location_model.basis_model
        return self.basis_selector.select(
            series.data["time"].to_numpy(dtype=float),
            series.values,
        )

    def _build_threshold_model(self, basis_model: BaseBasis) -> QuantileThresholdModel:
        """Build a threshold model tied to the selected basis model."""
        if self.threshold_selector is None:
            raise ValueError("threshold_selector must be available before building threshold model")
        template = self.threshold_selector.quantile_model
        return QuantileThresholdModel(
            include_trend=template.include_trend,
            n_harmonics=template.n_harmonics,
            basis_model=basis_model,
        )

    def _build_location_model(self, basis_model: BaseBasis) -> LocationModel:
        """Build a location model tied to the selected basis model."""
        if self.fitter is None:
            raise ValueError("fitter must be available before building location model")
        template = self.fitter.location_model
        return LocationModel(
            include_trend=template.include_trend,
            n_harmonics=template.n_harmonics,
            basis_model=basis_model,
        )

    def _prepare_series(
        self,
        data: pd.DataFrame | MonthlyTimeSeries,
        tail: TailDirection,
        frozen_year_months: set[int] | None = None,
    ) -> MonthlyTimeSeries:
        """Build the working series after defrozen preprocessing."""
        return (
            self._coerce_series(data)
            .defrozen(frozen_year_months)
            .validate_min_observations(self.min_observations)
            .for_tail(tail)
        )

    def estimate_threshold(
        self,
        data: pd.DataFrame | MonthlyTimeSeries,
        tail: TailDirection = "high",
        quantile: float = 0.90,
        frozen_year_months: set[int] | None = None,
    ) -> float:
        """Return the median of the fitted time-varying threshold as a scalar summary."""
        series = self._prepare_series(data, tail, frozen_year_months=frozen_year_months)
        basis_model = self._select_basis_model(series)
        threshold_model = self._build_threshold_model(basis_model)
        times = series.data["time"].to_numpy(dtype=float)
        params = threshold_model.fit(times, series.values, quantile=quantile)
        u_obs = threshold_model.evaluate(times, params)
        return float(np.median(u_obs))

    def threshold_diagnostics(
        self,
        data: pd.DataFrame | MonthlyTimeSeries,
        tail: TailDirection = "high",
        thresholds: np.ndarray | None = None,
        frozen_year_months: set[int] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Return MRL and parameter-stability diagnostics for threshold selection."""
        series = self._prepare_series(data, tail, frozen_year_months=frozen_year_months)
        return {
            "mrl": self.threshold_selector.mean_residual_life(series, thresholds=thresholds),
            "stability": self.threshold_selector.parameter_stability(
                series,
                thresholds=thresholds,
            ),
        }

    def prepare_extremes(
        self,
        data: pd.DataFrame | MonthlyTimeSeries,
        tail: TailDirection = "high",
        threshold: float | None = None,
        threshold_quantile: float = 0.90,
        frozen_year_months: set[int] | None = None,
        shared_raw_series: MonthlyTimeSeries | None = None,
        shared_defrozen_series: MonthlyTimeSeries | None = None,
    ) -> PreparedExtremes:
        """Transform the series, fit the threshold and decluster exceedances.

        When ``threshold`` is None (the default) a time-varying threshold is fitted
        using ``ThresholdSelector.fit_threshold``.  The returned ``threshold_params``
        can be evaluated at any time via ``ThresholdSelector.quantile_model.evaluate``,
        and the ``u_grid`` vector is pre-computed on the NHPP integration grid.

        Args:
            data: Raw frame or pre-built ``MonthlyTimeSeries``.
            tail: Tail direction.
            threshold: Optional fixed scalar threshold.  If given, the time-varying
                       model is bypassed.
            threshold_quantile: Quantile level for the seasonal + trend regression
                                (ignored when ``threshold`` is not None).

        Returns:
            A ``PreparedExtremes`` object with selected basis model, threshold fit
            artefacts and declustered exceedances.
        """
        raw_series = self._coerce_series(data) if shared_raw_series is None else shared_raw_series
        if shared_defrozen_series is None:
            shared = raw_series.defrozen(frozen_year_months).validate_min_observations(self.min_observations)
        else:
            shared = shared_defrozen_series.validate_min_observations(self.min_observations)
        series = shared.for_tail(tail)
        full_series = raw_series.for_tail(tail)
        basis_model = self._select_basis_model(series)

        if threshold is not None:
            u_obs = np.full(series.n_obs, float(threshold))
            rep_threshold = float(threshold)
            threshold_model: QuantileThresholdModel | None = None
            threshold_params: np.ndarray | None = None
            u_grid: np.ndarray | None = None
        else:
            threshold_model = self._build_threshold_model(basis_model)
            times = series.data["time"].to_numpy(dtype=float)
            threshold_params = threshold_model.fit(
                times,
                series.values,
                quantile=threshold_quantile,
            )
            u_obs = threshold_model.evaluate(times, threshold_params)
            rep_threshold = float(np.median(u_obs))
            # Pre-compute threshold on the NHPP integration grid
            if self.fitter is None:
                raise ValueError("fitter must be available before threshold grid evaluation")
            integration_grid = np.linspace(
                0.0, series.duration_years, self.fitter.integration_points
            )
            u_grid = threshold_model.evaluate(
                integration_grid,
                threshold_params,
            )
            plateau_schedule = build_frozen_plateau_schedule(
                frozen_year_months,
                int(full_series.data["year"].min()),
            )
            if plateau_schedule is not None:
                anchor_values = threshold_model.evaluate(
                    plateau_schedule.anchor_times,
                    threshold_params,
                )
                u_grid = apply_frozen_plateau(
                    integration_grid,
                    u_grid,
                    plateau_schedule,
                    anchor_values,
                )

        extremes = self.declustering_strategy.decluster(series, u_obs)
        return PreparedExtremes(
            series=series,
            representative_threshold=rep_threshold,
            extremes=extremes,
            basis_model=basis_model,
            threshold_model=threshold_model,
            threshold_params=threshold_params,
            u_grid=u_grid,
        )

    def _fit_from_prepared(
        self,
        prepared: PreparedExtremes,
        tail: TailDirection,
        source_data: pd.DataFrame | MonthlyTimeSeries,
        frozen_year_months: set[int] | None,
    ) -> FitResult:
        """Fit NHPP from already prepared threshold/declustering artifacts."""
        if prepared.extremes.empty:
            raise ValueError("No exceedances remain after thresholding and declustering")
        if self.fitter is None:
            raise ValueError("fitter must be available before fitting")
        location_model = self._build_location_model(prepared.basis_model)
        active_fitter = NHPPFitter(
            location_model=location_model,
            maxiter=self.fitter.maxiter,
            integration_points=self.fitter.integration_points,
            max_restarts=self.fitter.max_restarts,
            enable_powell_fallback=self.fitter.enable_powell_fallback,
        )
        return active_fitter.fit(
            series=prepared.series,
            extremes=prepared.extremes,
            threshold=prepared.representative_threshold,
            tail=tail,
            u_grid=prepared.u_grid,
            threshold_model=prepared.threshold_model,
            threshold_params=prepared.threshold_params,
            full_series=self._coerce_series(source_data).for_tail(tail),
            frozen_year_months=tuple(sorted(frozen_year_months or set())),
        )

    def fit(
        self,
        data: pd.DataFrame | MonthlyTimeSeries,
        tail: TailDirection = "high",
        threshold: float | None = None,
        threshold_quantile: float = 0.90,
        frozen_year_months: set[int] | None = None,
    ) -> FitResult:
        """Fit the NHPP model for high or low extremes."""
        prepared = self.prepare_extremes(
            data=data,
            tail=tail,
            threshold=threshold,
            threshold_quantile=threshold_quantile,
            frozen_year_months=frozen_year_months,
        )
        return self._fit_from_prepared(
            prepared=prepared,
            tail=tail,
            source_data=data,
            frozen_year_months=frozen_year_months,
        )

    def fit_both_tails(
        self,
        data: pd.DataFrame | MonthlyTimeSeries,
        threshold: float | None = None,
        threshold_quantile: float = 0.90,
        frozen_year_months: set[int] | None = None,
    ) -> tuple[FitResult, FitResult]:
        """Fit high and low tails with shared preprocessing artifacts."""
        raw_series = self._coerce_series(data)
        shared = raw_series.defrozen(frozen_year_months).validate_min_observations(self.min_observations)
        prepared_high = self.prepare_extremes(
            data=data,
            tail="high",
            threshold=threshold,
            threshold_quantile=threshold_quantile,
            frozen_year_months=frozen_year_months,
            shared_raw_series=raw_series,
            shared_defrozen_series=shared,
        )
        prepared_low = self.prepare_extremes(
            data=data,
            tail="low",
            threshold=threshold,
            threshold_quantile=threshold_quantile,
            frozen_year_months=frozen_year_months,
            shared_raw_series=raw_series,
            shared_defrozen_series=shared,
        )
        return (
            self._fit_from_prepared(
                prepared=prepared_high,
                tail="high",
                source_data=raw_series,
                frozen_year_months=frozen_year_months,
            ),
            self._fit_from_prepared(
                prepared=prepared_low,
                tail="low",
                source_data=raw_series,
                frozen_year_months=frozen_year_months,
            ),
        )


__all__ = [
    "LocationModel",
    "FitResult",
    "NHPPLogLikelihood",
    "NHPPFitter",
    "PreparedExtremes",
    "EOTEstimator",
]
