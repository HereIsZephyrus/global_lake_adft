from __future__ import annotations

import logging
import warnings

import numpy as np
import pandas as pd
from scipy.integrate import IntegrationWarning, quad
from scipy.optimize import minimize
from scipy.special import comb

try:
    from numba import njit
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    def njit(func):
        return func

from lakeanalysis.decomposition.base import DecompositionResult
from lakeanalysis.extreme.compute import (
    assign_extreme_labels,
    detect_abrupt_transitions,
    extract_extreme_events,
)
from lakeanalysis.extreme.models import ExtremeResult, PWMDiagnostics
from lakeanalysis.quality.frozen import filter_frozen_rows
from lakesource.pwm.schema import (
    PWMExtremeConfig,
    PWMExtremeMonthResult,
    PWMExtremeResult,
)

log = logging.getLogger(__name__)


def compute_pwm_beta(z_sorted: np.ndarray, K: int) -> np.ndarray:
    """Compute type-2 PWM b_0..b_K from sorted normalised observations."""
    n = len(z_sorted)
    if n < K + 1:
        raise ValueError(
            f"Need at least {K + 1} observations for PWM order K={K}; got {n}"
        )
    beta = np.zeros(K + 1, dtype=float)
    for k in range(K + 1):
        denom = comb(n - 1, k, exact=True)
        if denom == 0:
            beta[k] = 0.0
            continue
        weights = np.array([comb(i, k, exact=True) for i in range(n)], dtype=float)
        beta[k] = np.sum(weights * z_sorted) / (n * denom)
    return beta


def shifted_exponential_prior(u: np.ndarray, epsilon: float) -> np.ndarray:
    """Shifted-exponential prior quantile function y(u) = ε - (1-ε)ln(1-u)."""
    u = np.clip(u, 0.0, 1.0 - 1e-12)
    return epsilon - (1.0 - epsilon) * np.log(1.0 - u)


@njit
def _crossent_quantile_scalar(u: float, lam: np.ndarray, epsilon: float) -> float:
    """Fast scalar version for numba JIT."""
    u_clipped = min(max(u, 0.0), 1.0 - 1e-12)
    y = epsilon - (1.0 - epsilon) * np.log(1.0 - u_clipped)
    exp_term = 1.0
    u_pow = 1.0
    for j in range(len(lam)):
        exp_term *= np.exp(-lam[j] * u_pow)
        u_pow *= u_clipped
    return y * exp_term


def crossent_quantile(
    u: np.ndarray,
    lam: np.ndarray,
    epsilon: float,
) -> np.ndarray:
    """Cross-entropy quantile function x(u) = y(u) * exp(-Σ λ_j u^j)."""
    if HAS_NUMBA and u.ndim == 1 and len(u) > 0:
        result = np.empty(len(u), dtype=float)
        for i in range(len(u)):
            result[i] = _crossent_quantile_scalar(u[i], lam, epsilon)
        return result
    y = shifted_exponential_prior(u, epsilon)
    u_pow = np.column_stack([u**j for j in range(len(lam))])
    exp_term = np.exp(-u_pow @ lam)
    return y * exp_term


def _objective(
    lam: np.ndarray,
    b_target: np.ndarray,
    K: int,
    epsilon: float,
    config: PWMExtremeConfig,
) -> float:
    """Least-squares objective: sum of (integral_k - b_k)^2 + L2 penalty."""
    residuals = _compute_residuals(lam, b_target, K, epsilon, config)
    if not np.all(np.isfinite(residuals)):
        return 1e15 + config.l2_regularization * float(np.sum(lam**2))
    penalty = config.l2_regularization * float(np.sum(lam**2))
    return float(np.sum(residuals**2)) + penalty


def _compute_residuals(
    lam: np.ndarray,
    b_target: np.ndarray,
    K: int,
    epsilon: float,
    config: PWMExtremeConfig,
) -> np.ndarray:
    """Compute constraint residuals: integral_k - b_k for k=0..K."""
    residuals = np.zeros(K + 1, dtype=float)
    max_rel_err = 0.0
    for k in range(K + 1):
        if HAS_NUMBA:
            def integrand(u: float, _k: int = k) -> float:
                return (u**_k) * _crossent_quantile_scalar(u, lam, epsilon)
        else:
            def integrand(u: float, _k: int = k) -> float:
                x_val = float(crossent_quantile(np.array([u]), lam, epsilon)[0])
                return (u**_k) * x_val

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=IntegrationWarning)
            warnings.filterwarnings("ignore", category=RuntimeWarning)
            try:
                integral, err = quad(
                    integrand,
                    0.0,
                    config.integration_upper,
                    limit=500,
                )
            except Exception:
                integral = np.nan

        if not np.isfinite(integral):
            residuals[k] = 1e10
        else:
            if abs(integral) > 1e-15:
                max_rel_err = max(max_rel_err, abs(err / integral))
            residuals[k] = integral - b_target[k]
    if max_rel_err > 1e-6:
        log.debug("quad max relative error %.2e", max_rel_err)
    return residuals


def solve_lagrange_multipliers(
    b_target: np.ndarray,
    K: int,
    epsilon: float,
    config: PWMExtremeConfig | None = None,
) -> tuple[np.ndarray, bool, float]:
    """Solve for Lagrange multipliers via least-squares minimisation."""
    if config is None:
        config = PWMExtremeConfig()
    lam0 = np.zeros(K + 1, dtype=float)
    bounds = [(-100.0, 100.0)] * (K + 1)
    result = minimize(
        _objective,
        lam0,
        args=(b_target, K, epsilon, config),
        method="L-BFGS-B",
        bounds=bounds,
        options={"maxiter": 5000, "ftol": 1e-12},
    )
    return result.x.astype(float), bool(result.success), float(result.fun)


def compute_one_month_thresholds(
    area_values: np.ndarray,
    month: int,
    *,
    hylak_id: int | None = None,
    config: PWMExtremeConfig | None = None,
) -> PWMExtremeMonthResult:
    """Compute PWM cross-entropy thresholds for one month."""
    if config is None:
        config = PWMExtremeConfig()
    K = config.n_pwm
    mean_area = float(np.mean(area_values))
    if mean_area <= 0.0:
        raise ValueError(f"Mean area must be positive; got {mean_area}")

    z = area_values / mean_area
    z_sorted = np.sort(z)
    epsilon = float(z_sorted[0])

    b = compute_pwm_beta(z_sorted, K)
    lam_opt, converged, obj_val = solve_lagrange_multipliers(b, K, epsilon, config)

    u_high = 1.0 - config.p_high
    u_low = config.p_low
    x_high = float(crossent_quantile(np.array([u_high]), lam_opt, epsilon)[0])
    x_low = float(crossent_quantile(np.array([u_low]), lam_opt, epsilon)[0])

    return PWMExtremeMonthResult(
        hylak_id=hylak_id,
        month=month,
        threshold_quantile=1.0 - float(config.p_high),
        mean_area=mean_area,
        epsilon=epsilon,
        lambda_opt=lam_opt,
        pwm_coefficients=b,
        threshold_high=mean_area * x_high,
        threshold_low=mean_area * x_low,
        converged=converged,
        objective_value=obj_val,
    )


def assign_pwm_extreme_labels(
    index_df: pd.DataFrame,
    thresholds: dict[int, tuple[float, float]],
) -> pd.DataFrame:
    """Assign extreme labels based on PWM cross-entropy thresholds.

    Args:
        index_df: DataFrame with columns year, month, index_value.
        thresholds: Dict mapping month -> (threshold_low, threshold_high).

    Returns:
        DataFrame with added columns threshold_low, threshold_high, extreme_label.
    """
    if "index_value" not in index_df.columns:
        raise ValueError(
            "index_df lacks 'index_value' column — use assign_pwm_extreme_labels "
            "with an STL decomposition result"
        )
    labeled_df = index_df.copy()
    labeled_df["threshold_low"] = labeled_df["month"].map(
        lambda m: thresholds.get(m, (np.nan, np.nan))[0]
    )
    labeled_df["threshold_high"] = labeled_df["month"].map(
        lambda m: thresholds.get(m, (np.nan, np.nan))[1]
    )
    return assign_extreme_labels(labeled_df, labeled_df["threshold_low"], labeled_df["threshold_high"])


def compute_pooled_pwm_thresholds(
    result: DecompositionResult,
    *,
    hylak_id: int | None = None,
    config: PWMExtremeConfig | None = None,
) -> PWMExtremeResult:
    """Compute pooled PWM cross-entropy thresholds on ``index_value``."""
    if config is None:
        config = PWMExtremeConfig()

    index_df = result.index_df
    index_values = index_df["index_value"].to_numpy(dtype=float)

    if len(index_values) < config.min_observations_per_month:
        raise ValueError(
            "Insufficient observations for PWM estimation: "
            f"{len(index_values)} < {config.min_observations_per_month}"
        )

    month_results: list[PWMExtremeMonthResult] = []
    thresholds: dict[int, tuple[float, float]] = {}

    index_mean = float(np.mean(index_values))
    if index_mean <= 0.0:
        raise ValueError(f"Mean index_value must be positive; got {index_mean}")

    z = index_values / index_mean
    z_sorted = np.sort(z)
    epsilon = float(z_sorted[0])
    K = config.n_pwm

    b = compute_pwm_beta(z_sorted, K)
    lam_opt, converged, obj_val = solve_lagrange_multipliers(b, K, epsilon, config)

    u_high = 1.0 - config.p_high
    u_low = config.p_low
    x_high = float(crossent_quantile(np.array([u_high]), lam_opt, epsilon)[0])
    x_low = float(crossent_quantile(np.array([u_low]), lam_opt, epsilon)[0])

    threshold_high = index_mean * x_high
    threshold_low = index_mean * x_low

    for month in range(1, 13):
        mr = PWMExtremeMonthResult(
            hylak_id=hylak_id,
            month=month,
            threshold_quantile=1.0 - float(config.p_high),
            mean_area=0.0,
            epsilon=epsilon,
            lambda_opt=lam_opt,
            pwm_coefficients=b,
            threshold_high=threshold_high,
            threshold_low=threshold_low,
            converged=converged,
            objective_value=obj_val,
        )
        month_results.append(mr)
        thresholds[month] = (threshold_low, threshold_high)

    labeled_df = assign_pwm_extreme_labels(index_df, thresholds)
    labeled_df["threshold_quantile"] = 1.0 - float(config.p_high)
    if hylak_id is not None:
        labeled_df.insert(0, "hylak_id", hylak_id)
    else:
        labeled_df.insert(
            0, "hylak_id", pd.Series([pd.NA] * len(labeled_df), dtype="Int64")
        )

    extremes_df = _build_pwm_extremes(labeled_df)
    transitions_df = detect_abrupt_transitions(labeled_df, value_column="index_value")
    transitions_df = _rename_transition_columns(transitions_df)

    extreme = ExtremeResult(
        hylak_id=hylak_id,
        labels_df=labeled_df,
        extremes_df=extremes_df,
        transitions_df=transitions_df,
    )
    diagnostics = PWMDiagnostics(month_results=month_results)

    return PWMExtremeResult(extreme=extreme, diagnostics=diagnostics)


def _build_pwm_extremes(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Build PWM extremes with severity column from shared extraction."""
    extreme_df = extract_extreme_events(labeled_df)
    if not extreme_df.empty and "index_value" in extreme_df.columns and "threshold" in extreme_df.columns:
        extreme_df["severity"] = np.abs(
            extreme_df["index_value"].to_numpy(dtype=float)
            - extreme_df["threshold"].to_numpy(dtype=float)
        )
        cols = extreme_df.columns.tolist()
        if "severity" not in cols:
            extreme_df = extreme_df.assign(severity=extreme_df["severity"])
        extreme_df = extreme_df.reindex(
            columns=[
                "hylak_id", "threshold_quantile", "year", "month", "event_type", "water_area",
                "index_value", "threshold", "severity", "extreme_label",
            ]
        )
    return extreme_df


def _rename_transition_columns(transitions_df: pd.DataFrame) -> pd.DataFrame:
    """Rename shared column names to PWM store column names."""
    if transitions_df.empty:
        return transitions_df
    return transitions_df.rename(
        columns={
            "from_index_value": "from_water_area",
            "to_index_value": "to_water_area",
        }
    )


# ------------------------------------------------------------------
# Legacy raw water_area compatibility functions
# ------------------------------------------------------------------

def extract_pwm_extreme_events(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Extract extreme events from labelled raw-water_area data (legacy compat)."""
    warnings.warn(
        "extract_pwm_extreme_events is a legacy raw-water_area compatibility "
        "wrapper. Prefer PWMExtremeResult.extremes_df from the STL pipeline.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _extract_events_raw(labeled_df)


def detect_pwm_abrupt_transitions(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Detect abrupt transitions from labelled raw-water_area data (legacy compat)."""
    warnings.warn(
        "detect_pwm_abrupt_transitions is a legacy raw-water_area compatibility "
        "wrapper. Prefer PWMExtremeResult.transitions_df from the STL pipeline.",
        DeprecationWarning,
        stacklevel=2,
    )
    return _detect_transitions_raw(labeled_df)


def _assign_labels_raw(
    series_df: pd.DataFrame,
    thresholds: dict[int, tuple[float, float]],
) -> pd.DataFrame:
    """Assign extreme labels on raw water_area (legacy compat)."""
    df = series_df.copy()
    df["threshold_low"] = df["month"].map(lambda m: thresholds.get(m, (np.nan, np.nan))[0])
    df["threshold_high"] = df["month"].map(lambda m: thresholds.get(m, (np.nan, np.nan))[1])
    df["extreme_label"] = np.select(
        [df["water_area"] <= df["threshold_low"], df["water_area"] >= df["threshold_high"]],
        ["extreme_low", "extreme_high"],
        default="normal",
    )
    return df


def _extract_events_raw(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Extract extreme events on raw water_area (legacy compat)."""
    extreme_df = labeled_df.loc[labeled_df["extreme_label"] != "normal"].copy()
    if extreme_df.empty:
        return pd.DataFrame(
            columns=["hylak_id", "year", "month", "event_type", "water_area",
                     "threshold", "severity", "extreme_label"]
        )
    extreme_df["event_type"] = np.where(
        extreme_df["extreme_label"] == "extreme_high", "high", "low"
    )
    extreme_df["threshold"] = np.where(
        extreme_df["event_type"] == "high",
        extreme_df["threshold_high"],
        extreme_df["threshold_low"],
    )
    extreme_df["severity"] = np.abs(extreme_df["water_area"] - extreme_df["threshold"])
    return extreme_df.reindex(
        columns=["hylak_id", "year", "month", "event_type", "water_area",
                 "threshold", "severity", "extreme_label"]
    ).reset_index(drop=True)


def _detect_transitions_raw(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Detect abrupt transitions on raw water_area (legacy compat)."""
    ordered_df = labeled_df.sort_values(["year", "month"]).reset_index(drop=True)
    next_df = ordered_df.shift(-1)
    adjacency_mask = (next_df["month_ordinal"] - ordered_df["month_ordinal"]) == 1
    low_to_high = (
        (ordered_df["extreme_label"] == "extreme_low")
        & (next_df["extreme_label"] == "extreme_high") & adjacency_mask
    )
    high_to_low = (
        (ordered_df["extreme_label"] == "extreme_high")
        & (next_df["extreme_label"] == "extreme_low") & adjacency_mask
    )
    transition_mask = low_to_high | high_to_low
    if not transition_mask.any():
        return pd.DataFrame(columns=[
            "hylak_id", "from_year", "from_month", "to_year", "to_month",
            "transition_type", "from_water_area", "to_water_area", "from_label", "to_label",
        ])
    return pd.DataFrame({
        "hylak_id": ordered_df.loc[transition_mask, "hylak_id"].to_numpy(),
        "from_year": ordered_df.loc[transition_mask, "year"].to_numpy(dtype=int),
        "from_month": ordered_df.loc[transition_mask, "month"].to_numpy(dtype=int),
        "to_year": next_df.loc[transition_mask, "year"].to_numpy(dtype=int),
        "to_month": next_df.loc[transition_mask, "month"].to_numpy(dtype=int),
        "transition_type": np.where(
            low_to_high.loc[transition_mask].to_numpy(), "low_to_high", "high_to_low",
        ),
        "from_water_area": ordered_df.loc[transition_mask, "water_area"].to_numpy(dtype=float),
        "to_water_area": next_df.loc[transition_mask, "water_area"].to_numpy(dtype=float),
        "from_label": ordered_df.loc[transition_mask, "extreme_label"].to_numpy(),
        "to_label": next_df.loc[transition_mask, "extreme_label"].to_numpy(),
    }).reset_index(drop=True)


def compute_monthly_thresholds(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    config: PWMExtremeConfig | None = None,
    frozen_year_months: set[int] | None = None,
) -> PWMExtremeResult:
    """Compute per-month PWM thresholds on raw water_area (deprecated)."""
    warnings.warn(
        "compute_monthly_thresholds is deprecated; use compute_pooled_pwm_thresholds",
        DeprecationWarning,
        stacklevel=2,
    )

    if config is None:
        config = PWMExtremeConfig()

    df = series_df.copy()
    if frozen_year_months:
        df = filter_frozen_rows(df, frozen_year_months)

    if df.empty:
        raise ValueError("No observations remain after filtering frozen months")

    month_results: list[PWMExtremeMonthResult] = []
    thresholds: dict[int, tuple[float, float]] = {}

    for month in range(1, 13):
        month_values = df.loc[df["month"] == month, "water_area"].to_numpy(dtype=float)
        if len(month_values) < config.min_observations_per_month:
            log.debug(
                "Skipping month %d: %d observations < %d minimum",
                month,
                len(month_values),
                config.min_observations_per_month,
            )
            continue
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=IntegrationWarning)
                warnings.filterwarnings("ignore", category=RuntimeWarning)
                mr = compute_one_month_thresholds(
                    month_values,
                    month,
                    hylak_id=hylak_id,
                    config=config,
                )
            month_results.append(mr)
            thresholds[month] = (mr.threshold_low, mr.threshold_high)
        except (ValueError, RuntimeError, FloatingPointError) as exc:
            log.debug("PWM extreme failed for month %d: %s", month, exc)

    if not month_results:
        raise ValueError("No month had sufficient observations for PWM estimation")

    labeled_df = _assign_labels_raw(df, thresholds)
    if hylak_id is not None:
        labeled_df.insert(0, "hylak_id", hylak_id)
    else:
        labeled_df.insert(
            0, "hylak_id", pd.Series([pd.NA] * len(labeled_df), dtype="Int64")
        )

    labeled_df["year_month_key"] = labeled_df["year"] * 100 + labeled_df["month"]
    labeled_df["month_ordinal"] = labeled_df["year"] * 12 + (labeled_df["month"] - 1)

    extremes_df = _extract_events_raw(labeled_df)
    transitions_df = _detect_transitions_raw(labeled_df)

    extreme = ExtremeResult(
        hylak_id=hylak_id,
        labels_df=labeled_df,
        extremes_df=extremes_df,
        transitions_df=transitions_df,
    )
    diagnostics = PWMDiagnostics(month_results=month_results)

    return PWMExtremeResult(extreme=extreme, diagnostics=diagnostics)
