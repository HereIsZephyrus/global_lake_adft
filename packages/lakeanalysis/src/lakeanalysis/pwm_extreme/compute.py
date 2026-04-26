"""PWM + minimum cross-entropy extreme quantile estimation.

Implements the method from "Extreme quantile estimation using order statistics
with minimum cross-entropy principle": for each calendar month, compute
probability-weighted moments (PWM) from the observed series, then solve for
Lagrange multipliers that produce a quantile function minimising cross-entropy
against a shifted-exponential prior while matching the PWM constraints.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy.integrate import quad, IntegrationWarning
from scipy.optimize import minimize
from scipy.special import comb
import warnings

try:
    from numba import njit
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    def njit(func):
        return func

from lakeanalysis.quality.frozen import filter_frozen_rows
from lakesource.pwm_extreme.schema import (
    PWMExtremeConfig,
    PWMExtremeMonthResult,
    PWMExtremeResult,
)

log = logging.getLogger(__name__)


def compute_pwm_beta(z_sorted: np.ndarray, K: int) -> np.ndarray:
    """Compute type-2 PWM b_0..b_K from sorted normalised observations.

    Uses the unbiased estimator (equation 11 of the paper):
        b_k = (1/n) * sum_{i=1}^n [C(i-1, k) / C(n-1, k)] * z_{(i)}

    Args:
        z_sorted: Ascending-sorted normalised observations (mean = 1).
        K: Maximum PWM order (0..K).

    Returns:
        Array of length K+1 with b_0..b_K.
    """
    n = len(z_sorted)
    if n < K + 1:
        raise ValueError(f"Need at least {K + 1} observations for PWM order K={K}; got {n}")
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
    u_pow = np.column_stack([u ** j for j in range(len(lam))])
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
        return 1e15 + config.l2_regularization * float(np.sum(lam ** 2))
    penalty = config.l2_regularization * float(np.sum(lam ** 2))
    return float(np.sum(residuals ** 2)) + penalty


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
                return (u ** _k) * _crossent_quantile_scalar(u, lam, epsilon)
        else:
            def integrand(u: float, _k: int = k) -> float:
                x_val = float(crossent_quantile(np.array([u]), lam, epsilon)[0])
                return (u ** _k) * x_val

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
    """Solve for Lagrange multipliers via least-squares minimisation.

    Args:
        b_target: Target PWM values b_0..b_K.
        K: Maximum PWM order.
        epsilon: Shift parameter for the prior.
        config: Configuration (uses defaults if None).

    Returns:
        (lambda_opt, converged, objective_value).
    """
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
    """Compute PWM cross-entropy thresholds for one month.

    Args:
        area_values: Raw area observations for this month.
        month: Calendar month (1..12).
        hylak_id: Optional lake identifier.
        config: Configuration.

    Returns:
        A PWMExtremeMonthResult with thresholds and diagnostics.
    """
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
    series_df: pd.DataFrame,
    thresholds: dict[int, tuple[float, float]],
) -> pd.DataFrame:
    """Assign extreme labels based on PWM cross-entropy thresholds.

    Args:
        series_df: DataFrame with columns year, month, water_area.
        thresholds: Dict mapping month → (threshold_low, threshold_high).

    Returns:
        DataFrame with added columns threshold_low, threshold_high, extreme_label.
    """
    labeled_df = series_df.copy()
    labeled_df["threshold_low"] = labeled_df["month"].map(
        lambda m: thresholds.get(m, (np.nan, np.nan))[0]
    )
    labeled_df["threshold_high"] = labeled_df["month"].map(
        lambda m: thresholds.get(m, (np.nan, np.nan))[1]
    )
    labeled_df["extreme_label"] = np.select(
        [
            labeled_df["water_area"] <= labeled_df["threshold_low"],
            labeled_df["water_area"] >= labeled_df["threshold_high"],
        ],
        ["extreme_low", "extreme_high"],
        default="normal",
    )
    return labeled_df


def compute_monthly_thresholds(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    config: PWMExtremeConfig | None = None,
    frozen_year_months: set[int] | None = None,
) -> PWMExtremeResult:
    """Compute PWM cross-entropy thresholds for all 12 months of one lake.

    Args:
        series_df: DataFrame with columns year, month, water_area.
        hylak_id: Optional lake identifier.
        config: Configuration.
        frozen_year_months: Optional set of YYYYMM keys to exclude.

    Returns:
        A PWMExtremeResult with per-month thresholds and labelled series.
    """
    if config is None:
        config = PWMExtremeConfig()

    df = series_df.copy()
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

    labeled_df = assign_pwm_extreme_labels(df, thresholds)
    if hylak_id is not None:
        labeled_df.insert(0, "hylak_id", hylak_id)
    else:
        labeled_df.insert(0, "hylak_id", pd.Series([pd.NA] * len(labeled_df), dtype="Int64"))

    return PWMExtremeResult(
        hylak_id=hylak_id,
        month_results=month_results,
        labels_df=labeled_df,
    )
