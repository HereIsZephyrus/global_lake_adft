"""Diagnostics for PWM cross-entropy extreme quantile fits."""

from __future__ import annotations

import numpy as np
import pandas as pd

from .compute import crossent_quantile, shifted_exponential_prior


def quantile_function_curve(
    lambda_opt: np.ndarray,
    epsilon: float,
    n_points: int = 200,
) -> pd.DataFrame:
    """Return a DataFrame of (u, x(u)) for plotting the fitted quantile function.

    Args:
        lambda_opt: Fitted Lagrange multipliers.
        epsilon: Shift parameter.
        n_points: Number of evaluation points.

    Returns:
        DataFrame with columns u, prior_y, fitted_x.
    """
    u = np.linspace(0.0, 1.0 - 1e-8, n_points)
    prior_y = shifted_exponential_prior(u, epsilon)
    fitted_x = crossent_quantile(u, lambda_opt, epsilon)
    return pd.DataFrame({"u": u, "prior_y": prior_y, "fitted_x": fitted_x})


def pwm_constraint_residuals(
    lambda_opt: np.ndarray,
    b_target: np.ndarray,
    epsilon: float,
) -> pd.DataFrame:
    """Return per-order PWM constraint residuals for diagnostic inspection.

    Args:
        lambda_opt: Fitted Lagrange multipliers.
        b_target: Target PWM values.
        epsilon: Shift parameter.

    Returns:
        DataFrame with columns k, b_target, b_fitted, residual.
    """
    from scipy.integrate import quad

    K = len(b_target) - 1
    records: list[dict] = []
    for k in range(K + 1):
        def integrand(u: float, _k: int = k) -> float:
            x_val = float(crossent_quantile(np.array([u]), lambda_opt, epsilon)[0])
            return (u ** _k) * x_val

        integral, _ = quad(integrand, 0.0, 1.0 - 1e-10, limit=200)
        records.append({
            "k": k,
            "b_target": float(b_target[k]),
            "b_fitted": integral,
            "residual": integral - float(b_target[k]),
        })
    return pd.DataFrame(records)
