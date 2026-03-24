"""Type contracts for Hawkes modelling and model-comparison tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

import numpy as np
import pandas as pd


TYPE_DRY = 0
TYPE_WET = 1
TYPE_LABELS = ("D", "W")


@dataclass(frozen=True)
class HawkesEventSeries:
    """Container for two-type event times on a common time axis."""

    times: np.ndarray
    event_types: np.ndarray
    start_time: float
    end_time: float
    timeline: pd.DataFrame | None = None
    events_table: pd.DataFrame | None = None

    def __post_init__(self) -> None:
        times = np.asarray(self.times, dtype=float)
        event_types = np.asarray(self.event_types, dtype=int)
        if times.ndim != 1 or event_types.ndim != 1:
            raise ValueError("times and event_types must be 1-D arrays")
        if len(times) != len(event_types):
            raise ValueError("times and event_types must have the same length")
        if len(times) > 0 and np.any(np.diff(times) < 0.0):
            raise ValueError("times must be sorted in ascending order")
        if len(times) > 0 and (int(event_types.min()) < 0 or int(event_types.max()) > 1):
            raise ValueError("event_types must be in {0, 1}")
        if not np.isfinite(self.start_time) or not np.isfinite(self.end_time):
            raise ValueError("start_time and end_time must be finite")
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be greater than start_time")
        if len(times) > 0 and (float(times.min()) < self.start_time or float(times.max()) > self.end_time):
            raise ValueError("event times must be within [start_time, end_time]")

    @property
    def duration(self) -> float:
        """Observation horizon length."""
        return float(self.end_time - self.start_time)


@dataclass(frozen=True)
class HawkesModelSpec:
    """Model specification for a two-type exponential-kernel Hawkes process."""

    free_alpha_mask: np.ndarray = field(
        default_factory=lambda: np.ones((2, 2), dtype=bool)
    )
    kernel_window_years: float | None = 4.0 / 12.0
    enforce_stability: bool = True
    stability_penalty: float = 1e6

    def __post_init__(self) -> None:
        mask = np.asarray(self.free_alpha_mask, dtype=bool)
        if mask.shape != (2, 2):
            raise ValueError("free_alpha_mask must have shape (2, 2)")
        if self.kernel_window_years is not None and self.kernel_window_years <= 0.0:
            raise ValueError("kernel_window_years must be positive when provided")


@dataclass(frozen=True)
class HawkesFitResult:
    """Fitted parameter container."""

    theta: np.ndarray
    mu: np.ndarray
    alpha: np.ndarray
    beta: np.ndarray
    log_likelihood: float
    converged: bool
    message: str
    objective_value: float
    branching_matrix: np.ndarray
    spectral_radius: float
    model_spec: HawkesModelSpec
    event_series: HawkesEventSeries


@dataclass(frozen=True)
class LRTResult:
    """Likelihood-ratio test output."""

    test_name: str
    lr_statistic: float
    df: int
    p_value: float
    significance_level: float
    reject_null: bool
    restricted_log_likelihood: float
    full_log_likelihood: float


class ModelComparisonTest(Protocol):
    """Dependency-injected strategy interface for model comparison tests."""

    def compare(
        self,
        test_name: str,
        restricted_fit: HawkesFitResult,
        full_fit: HawkesFitResult,
        df: int,
    ) -> LRTResult:
        """Return model-comparison test output for a restricted/full fit pair."""

