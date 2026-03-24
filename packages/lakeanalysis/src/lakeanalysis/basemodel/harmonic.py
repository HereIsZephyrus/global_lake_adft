"""Trigonometric harmonic basis model."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .basic import BaseBasis


@dataclass(frozen=True)
class HarmonicBasis(BaseBasis):
    """Use sine and cosine harmonics as periodic basis features."""

    n_harmonics: int = 1

    def __post_init__(self) -> None:
        if self.n_harmonics < 1:
            raise ValueError("n_harmonics must be >= 1")

    @property
    def model_name(self) -> str:
        """Return the model identifier."""
        return f"harmonic_{self.n_harmonics}"

    @property
    def parameter_names(self) -> tuple[str, ...]:
        """Return basis parameter names."""
        names: list[str] = []
        for harmonic in range(1, self.n_harmonics + 1):
            names.extend([f"sin_{harmonic}", f"cos_{harmonic}"])
        return tuple(names)

    def design_columns(self, times: np.ndarray) -> np.ndarray:
        """Build harmonic columns for the supplied times."""
        times = np.asarray(times, dtype=float)
        columns: list[np.ndarray] = []
        for harmonic in range(1, self.n_harmonics + 1):
            frequency = 2.0 * np.pi * harmonic
            columns.append(np.sin(frequency * times))
            columns.append(np.cos(frequency * times))
        return np.column_stack(columns)
