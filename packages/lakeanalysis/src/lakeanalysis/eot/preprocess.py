"""Preprocessing utilities for excess-over-threshold point-process modelling."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Literal, Protocol

import numpy as np
import pandas as pd
from scipy.optimize import minimize
from scipy.stats import genpareto

from lakeanalysis.basemodel import BaseBasis, HarmonicBasis

log = logging.getLogger(__name__)

TailDirection = Literal["high", "low"]
MIN_OBSERVATIONS = 20


@dataclass(frozen=True)
class FrozenPlateauSchedule:
    """Frozen-period schedule for plateau-aligned model evaluation."""

    anchor_times: np.ndarray
    end_times: np.ndarray


def _year_month_to_key(year: int, month: int) -> int:
    """Return a YYYYMM integer key."""
    return year * 100 + month


def _year_month_key_to_index(year_month_key: int) -> int:
    """Convert a YYYYMM key to a continuous month index."""
    year = year_month_key // 100
    month = year_month_key % 100
    return year * 12 + (month - 1)


def _month_index_to_year_month_key(month_index: int) -> int:
    """Convert a continuous month index back to a YYYYMM key."""
    year = month_index // 12
    month = month_index % 12 + 1
    return _year_month_to_key(year, month)


def _frozen_run_indices(frozen_year_months: set[int] | None) -> list[tuple[int, int]]:
    """Return contiguous frozen-month runs as inclusive month-index intervals."""
    if not frozen_year_months:
        return []
    month_indices = sorted(_year_month_key_to_index(key) for key in frozen_year_months)
    runs: list[tuple[int, int]] = []
    run_start = month_indices[0]
    previous = month_indices[0]
    for month_index in month_indices[1:]:
        if month_index == previous + 1:
            previous = month_index
            continue
        runs.append((run_start, previous))
        run_start = month_index
        previous = month_index
    runs.append((run_start, previous))
    return runs


def first_frozen_months(frozen_year_months: set[int] | None) -> set[int]:
    """Return the first month of each contiguous frozen run."""
    return {
        _month_index_to_year_month_key(run_start)
        for run_start, _ in _frozen_run_indices(frozen_year_months)
    }


def build_frozen_plateau_schedule(
    frozen_year_months: set[int] | None,
    start_year: int,
) -> FrozenPlateauSchedule | None:
    """Build frozen-run time intervals used to hold fitted values constant."""
    runs = _frozen_run_indices(frozen_year_months)
    if not runs:
        return None
    anchor_times: list[float] = []
    end_times: list[float] = []
    for run_start, run_end in runs:
        start_key = _month_index_to_year_month_key(run_start)
        end_key = _month_index_to_year_month_key(run_end + 1)
        start_key_year = start_key // 100
        start_key_month = start_key % 100
        end_key_year = end_key // 100
        end_key_month = end_key % 100
        anchor_times.append(
            float(start_key_year - start_year) + float(start_key_month - 1) / 12.0
        )
        end_times.append(
            float(end_key_year - start_year) + float(end_key_month - 1) / 12.0
        )
    return FrozenPlateauSchedule(
        anchor_times=np.asarray(anchor_times, dtype=float),
        end_times=np.asarray(end_times, dtype=float),
    )


def apply_frozen_plateau(
    times: np.ndarray,
    values: np.ndarray,
    schedule: FrozenPlateauSchedule | None,
    anchor_values: np.ndarray | None,
) -> np.ndarray:
    """Hold fitted values constant across each frozen run."""
    adjusted = np.asarray(values, dtype=float).copy()
    if schedule is None:
        return adjusted
    if anchor_values is None:
        raise ValueError("anchor_values are required when a frozen plateau schedule is provided")
    times = np.asarray(times, dtype=float)
    anchor_values = np.asarray(anchor_values, dtype=float)
    if len(anchor_values) != len(schedule.anchor_times):
        raise ValueError("anchor_values length must match the frozen plateau schedule")
    epsilon = 1e-10
    for anchor_time, end_time, anchor_value in zip(
        schedule.anchor_times,
        schedule.end_times,
        anchor_values,
        strict=True,
    ):
        mask = (times >= anchor_time - epsilon) & (times < end_time - epsilon)
        adjusted[mask] = anchor_value
    return adjusted


@dataclass(frozen=True)
class MonthlyTimeSeries:
    """Monthly water-area series represented on a continuous time axis."""

    data: pd.DataFrame
    value_column: str = "water_area"
    direction: TailDirection = "high"

    @classmethod
    def from_frame(
        cls,
        df: pd.DataFrame,
        value_column: str = "water_area",
    ) -> "MonthlyTimeSeries":
        """Create a validated monthly time series from a lake-area frame.

        Args:
            df: Input frame with columns year, month and the value column.
            value_column: Column containing the monthly measurement.

        Returns:
            A validated ``MonthlyTimeSeries`` on the original high-tail scale.
        """
        required = {"year", "month", value_column}
        missing = required.difference(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")

        frame = (
            df.loc[:, ["year", "month", value_column]]
            .dropna(subset=["year", "month", value_column])
            .copy()
        )
        if frame.empty:
            raise ValueError("Input frame is empty after dropping missing values")

        frame["year"] = frame["year"].astype(int)
        frame["month"] = frame["month"].astype(int)
        frame["original_value"] = frame[value_column].astype(float)
        frame = frame.sort_values(["year", "month"]).reset_index(drop=True)

        invalid_months = frame.loc[~frame["month"].between(1, 12), "month"]
        if not invalid_months.empty:
            raise ValueError("Month values must be in the range 1..12")

        start_year = int(frame["year"].min())
        frame["time"] = (
            (frame["year"] - start_year).astype(float)
            + (frame["month"].astype(float) - 1.0) / 12.0
        )
        frame["value"] = frame["original_value"].astype(float)

        return cls(
            data=frame.loc[
                :, ["year", "month", "time", "value", "original_value"]
            ].reset_index(drop=True),
            value_column=value_column,
            direction="high",
        )

    def for_tail(self, direction: TailDirection) -> "MonthlyTimeSeries":
        """Return a view transformed for high- or low-tail modelling."""
        if direction == self.direction:
            return self

        frame = self.data.copy()
        if direction == "high":
            frame["value"] = frame["original_value"]
        else:
            # Negate working value, not original_value, to preserve any preprocessing
            frame["value"] = -frame["value"]

        return MonthlyTimeSeries(
            data=frame,
            value_column=self.value_column,
            direction=direction,
        )

    def defrozen(self, frozen_year_months: set[int] | None = None) -> "MonthlyTimeSeries":
        """Return a series with frozen-month observations removed.

        Args:
            frozen_year_months: Set of YYYYMM integers flagged as frozen in the
                anomaly table. ``None`` or an empty set leaves the series unchanged.

        Returns:
            A new ``MonthlyTimeSeries`` with frozen months removed while preserving
            the original continuous time coordinates of the remaining observations.
        """
        if not frozen_year_months:
            return self

        frame = self.data.copy()
        year_month = frame["year"].astype(int) * 100 + frame["month"].astype(int)
        retained_frozen = first_frozen_months(frozen_year_months)
        removed_frozen = set(frozen_year_months).difference(retained_frozen)
        result = frame.loc[~year_month.isin(removed_frozen)].reset_index(drop=True)
        if result.empty:
            raise ValueError("No observations remain after removing frozen months")
        log.debug(
            "defrozen removed %d month(s) while retaining %d frozen anchor month(s)",
            len(removed_frozen),
            len(retained_frozen),
        )
        return MonthlyTimeSeries(
            data=result,
            value_column=self.value_column,
            direction=self.direction,
        )

    def validate_min_observations(
        self,
        min_observations: int = MIN_OBSERVATIONS,
    ) -> "MonthlyTimeSeries":
        """Validate that the series contains enough observations for EOT fitting."""
        if self.n_obs < min_observations:
            raise ValueError(
                f"At least {min_observations} observations are required after preprocessing; "
                f"got {self.n_obs}"
            )
        return self

    @property
    def n_obs(self) -> int:
        """Return the number of monthly observations."""
        return len(self.data)

    @property
    def duration_years(self) -> float:
        """Return the observed time span in years."""
        if self.data.empty:
            return 0.0
        times = self.data["time"].to_numpy(dtype=float)
        return float(times.max() - times.min() + 1.0 / 12.0)

    @property
    def values(self) -> np.ndarray:
        """Return the working values used for tail modelling."""
        return self.data["value"].to_numpy(dtype=float)

    @property
    def original_values(self) -> np.ndarray:
        """Return the untransformed values."""
        return self.data["original_value"].to_numpy(dtype=float)


@dataclass(frozen=True)
class QuantileThresholdModel:
    """Seasonal and trend quantile regression for a time-varying exceedance threshold.

    Uses the same parametric design as ``LocationModel`` (intercept + optional linear
    trend + injected periodic basis columns), but estimated at a given quantile via
    the pinball (check-function) loss instead of the mean.

    The fitted threshold u(t) satisfies P(X(t) > u(t)) ≈ 1 - quantile at each time
    point, capturing both seasonal and long-term non-stationarity.  This is the basis
    for all exceedance decisions (declustering) and for the integral term of the NHPP
    log-likelihood.
    """

    include_trend: bool = True
    n_harmonics: int = 1
    basis_model: BaseBasis | None = None

    def __post_init__(self) -> None:
        """Initialise the injected basis model while preserving legacy arguments."""
        if self.n_harmonics < 1:
            raise ValueError("n_harmonics must be >= 1")
        if self.basis_model is None:
            object.__setattr__(self, "basis_model", HarmonicBasis(self.n_harmonics))

    def design_matrix(self, times: np.ndarray) -> np.ndarray:
        """Build the design matrix with the injected basis model."""
        if self.basis_model is None:
            raise ValueError("basis_model must be initialised before building the design matrix")
        return self.basis_model.build_design_matrix(
            np.asarray(times, dtype=float),
            include_trend=self.include_trend,
            include_intercept=True,
        )

    def fit(
        self,
        times: np.ndarray,
        values: np.ndarray,
        quantile: float,
    ) -> np.ndarray:
        """Fit quantile regression and return the parameter vector.

        Minimises the pinball (check-function) loss using L-BFGS-B, initialised
        from the ordinary least-squares solution.

        Args:
            times: Continuous time values for each observation.
            values: Working tail values (already transformed for the chosen tail).
            quantile: Target quantile in (0, 1), e.g. 0.90.

        Returns:
            1-D array of fitted parameter values (length = design matrix columns).
        """
        if not 0.0 < quantile < 1.0:
            raise ValueError("quantile must be in (0, 1)")
        times = np.asarray(times, dtype=float)
        values = np.asarray(values, dtype=float)
        design = self.design_matrix(times)

        def _loss(params: np.ndarray) -> float:
            residuals = values - design @ params
            return float(
                np.mean(np.where(residuals >= 0.0, quantile * residuals, (quantile - 1.0) * residuals))
            )

        def _gradient(params: np.ndarray) -> np.ndarray:
            residuals = values - design @ params
            signs = np.where(residuals >= 0.0, -quantile, 1.0 - quantile)
            return (design.T @ signs) / len(values)

        init_params, *_ = np.linalg.lstsq(design, values, rcond=None)
        result = minimize(_loss, init_params, jac=_gradient, method="L-BFGS-B")
        if not result.success:
            log.debug(
                "QuantileThresholdModel.fit: optimisation did not fully converge (%s); "
                "using best found solution.",
                result.message,
            )
        return result.x.astype(float)

    def evaluate(self, times: np.ndarray, params: np.ndarray) -> np.ndarray:
        """Evaluate the fitted threshold model at arbitrary time points.

        Args:
            times: Time values at which to evaluate u(t).
            params: Parameter vector returned by ``fit``.

        Returns:
            1-D array of threshold values u(t_i).
        """
        return self.design_matrix(np.asarray(times, dtype=float)) @ np.asarray(params, dtype=float)


class DeclusteringStrategy(Protocol):
    """Interface for declustering exceedances before NHPP fitting."""

    def decluster(
        self,
        series: MonthlyTimeSeries,
        threshold: float | np.ndarray,
    ) -> pd.DataFrame:
        """Return one representative exceedance per cluster."""


@dataclass(frozen=True)
class NoDeclustering:
    """Keep all exceedances and assign one singleton cluster per point."""

    def decluster(
        self,
        series: MonthlyTimeSeries,
        threshold: float | np.ndarray,
    ) -> pd.DataFrame:
        """Return all exceedances without merging adjacent points."""
        frame = series.data.reset_index(drop=True)
        values = frame["value"].to_numpy(dtype=float)
        threshold_array = _broadcast_threshold(threshold, len(frame))
        columns = [
            "cluster_id",
            "cluster_size",
            "year",
            "month",
            "time",
            "value",
            "original_value",
            "threshold",
        ]
        mask = values > threshold_array
        exceedances = frame.loc[mask].copy()
        if exceedances.empty:
            return pd.DataFrame(columns=columns)

        exceedances["cluster_id"] = np.arange(1, len(exceedances) + 1, dtype=int)
        exceedances["cluster_size"] = 1
        exceedances["threshold"] = threshold_array[mask]
        result = exceedances.loc[:, columns].reset_index(drop=True)
        log.debug("NoDeclustering retained all %d exceedances", len(result))
        return result


@dataclass(frozen=True)
class RunsDeclustering:
    """Runs declustering with configurable run length."""

    run_length: int = 1

    def __post_init__(self) -> None:
        if self.run_length < 1:
            raise ValueError("run_length must be >= 1")

    def decluster(
        self,
        series: MonthlyTimeSeries,
        threshold: float | np.ndarray,
    ) -> pd.DataFrame:
        """Decluster exceedances using the runs rule.

        Consecutive exceedances belong to the same cluster if they are separated
        by fewer than ``run_length`` non-exceedance observations.  Accepts either
        a scalar threshold or a per-observation threshold array (time-varying).
        """
        frame = series.data.reset_index(drop=True)
        values = frame["value"].to_numpy(dtype=float)
        threshold_array = _broadcast_threshold(threshold, len(frame))

        mask = values > threshold_array
        exceedance_indices = np.flatnonzero(mask)
        columns = [
            "cluster_id",
            "cluster_size",
            "year",
            "month",
            "time",
            "value",
            "original_value",
            "threshold",
        ]
        if exceedance_indices.size == 0:
            return pd.DataFrame(columns=columns)

        clusters: list[list[int]] = [[int(exceedance_indices[0])]]
        for raw_index in exceedance_indices[1:]:
            current_index = int(raw_index)
            previous_index = clusters[-1][-1]
            gap = current_index - previous_index - 1
            if gap >= self.run_length:
                clusters.append([current_index])
            else:
                clusters[-1].append(current_index)

        records: list[dict] = []
        for cluster_id, cluster in enumerate(clusters, start=1):
            cluster_frame = frame.iloc[cluster]
            representative_idx = int(cluster_frame["value"].idxmax())
            representative = frame.loc[representative_idx]
            records.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_size": len(cluster),
                    "year": int(representative["year"]),
                    "month": int(representative["month"]),
                    "time": float(representative["time"]),
                    "value": float(representative["value"]),
                    "original_value": float(representative["original_value"]),
                    "threshold": float(threshold_array[representative_idx]),
                }
            )

        result = pd.DataFrame(records, columns=columns)
        log.debug(
            "RunsDeclustering retained %d clusters from %d exceedances",
            len(result),
            int(exceedance_indices.size),
        )
        return result


def _broadcast_threshold(threshold: float | np.ndarray, n: int) -> np.ndarray:
    """Return threshold as a 1-D array of length n."""
    if np.isscalar(threshold):
        return np.full(n, float(threshold), dtype=float)
    arr = np.asarray(threshold, dtype=float)
    if arr.ndim != 1 or len(arr) != n:
        raise ValueError(
            f"threshold array length {len(arr)} does not match series length {n}"
        )
    return arr


@dataclass(frozen=True)
class ThresholdSelector:
    """Threshold diagnostics based on MRL and GPD stability.

    The primary threshold selection mechanism is the ``QuantileThresholdModel``,
    which fits a seasonal + trend quantile regression to produce a time-varying
    threshold u(t).  The scalar MRL and parameter-stability diagnostics remain
    available as supporting tools for visual inspection.
    """

    min_exceedances: int = 8
    lower_quantile: float = 0.70
    upper_quantile: float = 0.98
    n_thresholds: int = 25
    quantile_model: QuantileThresholdModel = field(default_factory=QuantileThresholdModel)

    def candidate_thresholds(self, series: MonthlyTimeSeries) -> np.ndarray:
        """Return candidate thresholds on an evenly spaced quantile grid."""
        quantiles = np.linspace(
            self.lower_quantile,
            self.upper_quantile,
            self.n_thresholds,
        )
        thresholds = np.quantile(series.values, quantiles)
        return np.unique(thresholds.astype(float))

    def mean_residual_life(
        self,
        series: MonthlyTimeSeries,
        thresholds: np.ndarray | None = None,
    ) -> pd.DataFrame:
        """Compute mean residual life diagnostics for a grid of thresholds."""
        threshold_grid = (
            self.candidate_thresholds(series)
            if thresholds is None
            else np.asarray(thresholds, dtype=float)
        )
        values = series.values
        records: list[dict] = []
        for threshold in threshold_grid:
            exceedances = values[values > threshold] - threshold
            mean_excess = (
                float(np.mean(exceedances))
                if exceedances.size >= self.min_exceedances
                else float("nan")
            )
            records.append(
                {
                    "threshold": float(threshold),
                    "mean_excess": mean_excess,
                    "n_exceedances": int(exceedances.size),
                }
            )
        return pd.DataFrame(records)

    def parameter_stability(
        self,
        series: MonthlyTimeSeries,
        thresholds: np.ndarray | None = None,
    ) -> pd.DataFrame:
        """Fit a GPD at each threshold to inspect parameter stability."""
        threshold_grid = (
            self.candidate_thresholds(series)
            if thresholds is None
            else np.asarray(thresholds, dtype=float)
        )
        values = series.values
        records: list[dict] = []
        for threshold in threshold_grid:
            exceedances = values[values > threshold] - threshold
            if exceedances.size < self.min_exceedances:
                records.append(
                    {
                        "threshold": float(threshold),
                        "shape_xi": float("nan"),
                        "scale_sigma_u": float("nan"),
                        "modified_scale": float("nan"),
                        "n_exceedances": int(exceedances.size),
                    }
                )
                continue

            try:
                shape, _, scale = genpareto.fit(exceedances, floc=0.0)
            except (RuntimeError, ValueError):
                shape = float("nan")
                scale = float("nan")

            modified_scale = (
                float(scale - shape * threshold)
                if np.isfinite(shape) and np.isfinite(scale)
                else float("nan")
            )
            records.append(
                {
                    "threshold": float(threshold),
                    "shape_xi": float(shape),
                    "scale_sigma_u": float(scale),
                    "modified_scale": modified_scale,
                    "n_exceedances": int(exceedances.size),
                }
            )

        return pd.DataFrame(records)

    def fit_threshold(
        self,
        series: MonthlyTimeSeries,
        quantile: float = 0.90,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Fit a time-varying threshold and return (params, u_obs).

        Args:
            series: Monthly time series (working-tail scale).
            quantile: Target exceedance quantile, e.g. 0.90 → u(t) is the local
                      90th-percentile curve.

        Returns:
            params: Parameter vector of the fitted ``QuantileThresholdModel``.
            u_obs:  Threshold value at each observation time (same length as series).
        """
        times = series.data["time"].to_numpy(dtype=float)
        values = series.values
        params = self.quantile_model.fit(times, values, quantile)
        u_obs = self.quantile_model.evaluate(times, params)
        log.debug(
            "fit_threshold: quantile=%.2f, u(t) range [%.4g, %.4g]",
            quantile,
            float(u_obs.min()),
            float(u_obs.max()),
        )
        return params, u_obs

    def suggest_threshold(
        self,
        series: MonthlyTimeSeries,
        quantile: float = 0.90,
    ) -> float:
        """Return the median of the fitted time-varying threshold as a scalar summary.

        This scalar is used only for initialisation and diagnostic display; all
        exceedance decisions and NHPP computations use the full u(t) vector from
        ``fit_threshold``.
        """
        if not 0.0 < quantile < 1.0:
            raise ValueError("quantile must be in (0, 1)")
        _, u_obs = self.fit_threshold(series, quantile)
        return float(np.median(u_obs))
