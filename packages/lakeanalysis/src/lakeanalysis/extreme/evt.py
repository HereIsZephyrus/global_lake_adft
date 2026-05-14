"""Shared EVT algorithm helpers.

These are pure numerical / row-building utilities shared by method-specific
adapters such as EOT and PWM. This module intentionally does not orchestrate
method flows or route dispatch.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import genpareto

DEFAULT_RETURN_PERIODS = (2, 5, 10, 20)

ROUTE_A = "A"
ROUTE_B = "B"

EVT_SUMMARY_COLS = [
    "tail",
    "threshold",
    "n_total",
    "n_exceedances",
    "shape",
    "scale",
    "converged",
    "error_message",
    "return_period",
    "return_level",
    "evt_route",
    "strength_unit",
]

EVT_STRENGTH_COLS = [
    "year",
    "month",
    "tail",
    "threshold",
    "exceedance",
    "event_strength",
]


def fit_gpd_exceedances(exceedances: np.ndarray) -> tuple[float, float]:
    """Fit a Generalized Pareto Distribution to non-negative exceedances."""
    if len(exceedances) < 3:
        raise ValueError("Need at least 3 exceedances for GPD fit")
    if np.any(exceedances < 0.0):
        raise ValueError("Exceedances must be non-negative")
    shape, _, scale = genpareto.fit(exceedances, floc=0.0)
    if not np.isfinite(shape) or not np.isfinite(scale) or scale <= 0.0:
        raise ValueError("GPD fit produced invalid parameters")
    return float(shape), float(scale)


def compute_return_level(  # pylint: disable=too-many-arguments
    *,
    threshold: float,
    shape: float,
    scale: float,
    return_period: int,
    n_total: int,
    n_exceedances: int,
) -> float:
    """Compute the return level for a given return period."""
    rate = (return_period * n_exceedances) / max(n_total, 1)
    if rate <= 1.0:
        return float(threshold)
    if abs(shape) < 1e-12:
        return float(threshold + scale * np.log(rate))
    return float(threshold + (scale / shape) * (rate**shape - 1.0))


def build_empty_tail_summary_rows(
    *,
    tail: str,
    n_total: int,
    return_periods: tuple[int, ...],
    evt_route: str,
    strength_unit: str,
) -> list[dict]:
    """Build summary rows when no exceedances are available."""
    return [
        {
            "tail": tail,
            "threshold": None,
            "n_total": n_total,
            "n_exceedances": 0,
            "shape": None,
            "scale": None,
            "converged": False,
            "error_message": "No exceedances",
            "return_period": period,
            "return_level": None,
            "evt_route": evt_route,
            "strength_unit": strength_unit,
        }
        for period in return_periods
    ]


def build_fitted_tail_summary_rows(  # pylint: disable=too-many-arguments
    tail_df: pd.DataFrame,
    *,
    tail: str,
    n_total: int,
    return_periods: tuple[int, ...],
    evt_route: str,
    strength_unit: str,
    threshold_column: str = "threshold",
    exceedance_column: str = "exceedance",
) -> list[dict]:
    """Build summary rows with GPD fit and return levels for one tail."""
    if tail_df.empty:
        return build_empty_tail_summary_rows(
            tail=tail,
            n_total=n_total,
            return_periods=return_periods,
            evt_route=evt_route,
            strength_unit=strength_unit,
        )

    threshold = float(tail_df[threshold_column].iloc[0])
    exceedances = tail_df[exceedance_column].to_numpy(dtype=float)
    try:
        shape, scale = fit_gpd_exceedances(exceedances)
        return [
            {
                "tail": tail,
                "threshold": threshold,
                "n_total": n_total,
                "n_exceedances": len(exceedances),
                "shape": shape,
                "scale": scale,
                "converged": True,
                "error_message": None,
                "return_period": period,
                "return_level": compute_return_level(
                    threshold=threshold,
                    shape=shape,
                    scale=scale,
                    return_period=period,
                    n_total=n_total,
                    n_exceedances=len(exceedances),
                ),
                "evt_route": evt_route,
                "strength_unit": strength_unit,
            }
            for period in return_periods
        ]
    except ValueError as exc:
        return [
            {
                "tail": tail,
                "threshold": threshold,
                "n_total": n_total,
                "n_exceedances": len(exceedances),
                "shape": None,
                "scale": None,
                "converged": False,
                "error_message": str(exc),
                "return_period": period,
                "return_level": None,
                "evt_route": evt_route,
                "strength_unit": strength_unit,
            }
            for period in return_periods
        ]
