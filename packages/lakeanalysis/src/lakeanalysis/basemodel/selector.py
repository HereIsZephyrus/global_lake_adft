"""Automatic baseline model selection."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging

import numpy as np

from .basic import BaseBasis, BasisFitRecord
from .harmonic import HarmonicBasis

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BasisSelectionResult:
    """Selection diagnostics for one time series."""

    selected_basis: BaseBasis
    selected_record: BasisFitRecord | None
    candidate_records: tuple[BasisFitRecord, ...]
    criterion: str
    value_scale: float
    relative_rmse: float | None
    fallback_reason: str | None = None

    @property
    def used_fallback(self) -> bool:
        """Return True when selector had to degrade from ideal logic."""
        return self.fallback_reason is not None


@dataclass(frozen=True)
class BasisSelector:
    """Select a periodic basis model for a time series."""

    candidates: tuple[BaseBasis, ...] = field(
        default_factory=lambda: (
            HarmonicBasis(n_harmonics=1),
            HarmonicBasis(n_harmonics=2),
            HarmonicBasis(n_harmonics=3),
        )
    )
    criterion: str = "aic"
    include_trend: bool = True
    max_relative_rmse: float = 1.0

    def __post_init__(self) -> None:
        if self.criterion not in {"aic", "bic"}:
            raise ValueError("criterion must be either 'aic' or 'bic'")
        if self.max_relative_rmse <= 0.0:
            raise ValueError("max_relative_rmse must be > 0")
        if not self.candidates:
            raise ValueError("At least one candidate basis model is required")

    def _fit_record(self, times: np.ndarray, values: np.ndarray, basis: BaseBasis) -> BasisFitRecord:
        """Fit one candidate basis by least squares and return diagnostics."""
        design = basis.build_design_matrix(times, include_trend=self.include_trend)
        n_obs = len(values)
        n_params = design.shape[1]
        if n_obs <= n_params:
            return BasisFitRecord(
                basis_name=basis.model_name,
                rmse=float("inf"),
                aic=float("inf"),
                bic=float("inf"),
                n_params=n_params,
                converged=False,
                message="Insufficient samples for model complexity",
            )
        try:
            params, *_ = np.linalg.lstsq(design, values, rcond=None)
            residuals = values - design @ params
            rss = float(np.dot(residuals, residuals))
            mse = max(rss / n_obs, np.finfo(float).tiny)
            rmse = float(np.sqrt(mse))
            aic = float(n_obs * np.log(mse) + 2.0 * n_params)
            bic = float(n_obs * np.log(mse) + np.log(n_obs) * n_params)
            return BasisFitRecord(
                basis_name=basis.model_name,
                rmse=rmse,
                aic=aic,
                bic=bic,
                n_params=n_params,
                converged=True,
            )
        except np.linalg.LinAlgError as exc:
            return BasisFitRecord(
                basis_name=basis.model_name,
                rmse=float("inf"),
                aic=float("inf"),
                bic=float("inf"),
                n_params=n_params,
                converged=False,
                message=f"Least-squares failure: {exc}",
            )

    def fit_basis(
        self,
        times: np.ndarray,
        values: np.ndarray,
        basis: BaseBasis,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Fit one basis model and return (params, fitted, residuals)."""
        times = np.asarray(times, dtype=float)
        values = np.asarray(values, dtype=float)
        design = basis.build_design_matrix(times, include_trend=self.include_trend)
        params, *_ = np.linalg.lstsq(design, values, rcond=None)
        fitted = design @ params
        residuals = values - fitted
        return params.astype(float), fitted.astype(float), residuals.astype(float)

    def select_result(self, times: np.ndarray, values: np.ndarray) -> BasisSelectionResult:
        """Return full selection diagnostics for one time series."""
        times = np.asarray(times, dtype=float)
        values = np.asarray(values, dtype=float)
        if times.ndim != 1 or values.ndim != 1 or len(times) != len(values):
            raise ValueError("times and values must be one-dimensional arrays of equal length")
        if len(times) < 3:
            raise ValueError("Too few observations to select a periodic basis.")

        records = tuple(self._fit_record(times, values, basis) for basis in self.candidates)
        scored = [record for record in records if record.converged and np.isfinite(record.rmse)]
        if not scored:
            raise ValueError("No candidate harmonic basis converged during selection.")

        if self.criterion == "bic":
            best_record = min(scored, key=lambda item: item.bic)
        else:
            best_record = min(scored, key=lambda item: item.aic)

        value_scale = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
        if value_scale <= np.finfo(float).tiny:
            value_scale = 1.0
        relative_rmse = best_record.rmse / value_scale
        selected_basis = next(
            basis for basis in self.candidates if basis.model_name == best_record.basis_name
        )

        if relative_rmse > self.max_relative_rmse:
            reason = (
                "Best candidate error is too large: "
                f"relative_rmse={relative_rmse:.4g} > max_relative_rmse={self.max_relative_rmse:.4g}"
            )
            log.warning("Basis selection degrades to best harmonic candidate: %s", reason)
            return BasisSelectionResult(
                selected_basis=selected_basis,
                selected_record=best_record,
                candidate_records=records,
                criterion=self.criterion,
                value_scale=value_scale,
                relative_rmse=relative_rmse,
                fallback_reason=reason,
            )
        log.debug(
            "Selected basis '%s' with %s=%.4f and rmse=%.4f",
            best_record.basis_name,
            self.criterion,
            best_record.bic if self.criterion == "bic" else best_record.aic,
            best_record.rmse,
        )
        return BasisSelectionResult(
            selected_basis=selected_basis,
            selected_record=best_record,
            candidate_records=records,
            criterion=self.criterion,
            value_scale=value_scale,
            relative_rmse=relative_rmse,
            fallback_reason=None,
        )

    def select(self, times: np.ndarray, values: np.ndarray) -> BaseBasis:
        """Return the selected harmonic basis model."""
        return self.select_result(times, values).selected_basis
