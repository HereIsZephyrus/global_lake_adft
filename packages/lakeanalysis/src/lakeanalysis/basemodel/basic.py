"""Abstract baseline model definitions."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class BasisFitRecord:
    """Store fit diagnostics for one candidate basis model."""

    basis_name: str
    rmse: float
    aic: float
    bic: float
    n_params: int
    converged: bool
    message: str = ""


class BaseBasis(ABC):
    """Abstract periodic basis model used by EOT design matrices."""

    @property
    @abstractmethod
    def model_name(self) -> str:
        """Return a short model identifier."""

    @property
    @abstractmethod
    def parameter_names(self) -> tuple[str, ...]:
        """Return ordered names for basis-specific parameters."""

    @abstractmethod
    def design_columns(self, times: np.ndarray) -> np.ndarray:
        """Return basis-only design columns with shape (n_obs, n_features)."""

    @property
    def n_features(self) -> int:
        """Return the number of basis-specific features."""
        return len(self.parameter_names)

    def build_design_matrix(
        self,
        times: np.ndarray,
        include_trend: bool = True,
        include_intercept: bool = True,
    ) -> np.ndarray:
        """Build a full linear design matrix from this basis."""
        times = np.asarray(times, dtype=float)
        columns: list[np.ndarray] = []
        if include_intercept:
            columns.append(np.ones_like(times))
        if include_trend:
            columns.append(times)
        basis_columns = self.design_columns(times)
        if basis_columns.ndim != 2 or basis_columns.shape[0] != len(times):
            raise ValueError(
                "design_columns must return a 2-D array with row count equal to len(times)"
            )
        if basis_columns.shape[1] > 0:
            columns.extend([basis_columns[:, idx] for idx in range(basis_columns.shape[1])])
        return np.column_stack(columns)
