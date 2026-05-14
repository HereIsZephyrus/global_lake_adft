"""Route B EVT helpers on continuous amplitude space."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import genpareto


DEFAULT_RETURN_PERIODS = (2, 5, 10, 20)


def _fit_gpd(exceedances: np.ndarray) -> tuple[float, float]:
    if len(exceedances) < 3:
        raise ValueError("Need at least 3 exceedances for GPD fit")
    if np.any(exceedances < 0.0):
        raise ValueError("Exceedances must be non-negative")
    shape, _, scale = genpareto.fit(exceedances, floc=0.0)
    if not np.isfinite(shape) or not np.isfinite(scale) or scale <= 0.0:
        raise ValueError("GPD fit produced invalid parameters")
    return float(shape), float(scale)


def _return_level(
    *,
    threshold: float,
    shape: float,
    scale: float,
    return_period: int,
    n_total: int,
    n_exceedances: int,
) -> float:
    rate = (return_period * n_exceedances) / max(n_total, 1)
    if rate <= 1.0:
        return float(threshold)
    if abs(shape) < 1e-12:
        return float(threshold + scale * np.log(rate))
    return float(threshold + (scale / shape) * (rate**shape - 1.0))


def _tail_threshold(values: np.ndarray, tail: str) -> float:
    if tail == "high":
        return float(np.min(values))
    return float(np.max(values))


def _build_tail_rows(
    tail_df: pd.DataFrame,
    *,
    tail: str,
    n_total: int,
    route: str,
    strength_unit: str,
    return_periods: tuple[int, ...],
) -> list[dict]:
    if tail_df.empty:
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
                "evt_route": route,
                "strength_unit": strength_unit,
            }
            for period in return_periods
        ]

    threshold = float(tail_df["amplitude_threshold"].iloc[0])
    exceedances = tail_df["exceedance"].to_numpy(dtype=float)
    try:
        shape, scale = _fit_gpd(exceedances)
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
                "return_level": _return_level(
                    threshold=threshold,
                    shape=shape,
                    scale=scale,
                    return_period=period,
                    n_total=n_total,
                    n_exceedances=len(exceedances),
                ),
                "evt_route": route,
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
                "evt_route": route,
                "strength_unit": strength_unit,
            }
            for period in return_periods
        ]


def compute_evt_amplitude_strengths(
    labeled_df: pd.DataFrame,
    *,
    amplitude_column: str = "stl_residual",
    return_periods: tuple[int, ...] = DEFAULT_RETURN_PERIODS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute Route B strengths using a continuous amplitude coordinate.

    The current minimal implementation derives an amplitude-space threshold from
    the PWM-labeled extreme months themselves:

    - high tail threshold: minimum amplitude among ``extreme_high`` months
    - low tail threshold: maximum amplitude among ``extreme_low`` months

    Strength remains the raw amplitude exceedance, while GPD fit and return
    levels are emitted as diagnostics / persisted outputs.
    """
    required_cols = {
        "year",
        "month",
        "extreme_label",
        amplitude_column,
    }
    missing = required_cols - set(labeled_df.columns)
    if missing:
        raise ValueError(f"labeled_df missing required columns: {sorted(missing)}")

    df = labeled_df.copy()
    high_df = df[df["extreme_label"] == "extreme_high"].copy()
    low_df = df[df["extreme_label"] == "extreme_low"].copy()
    empty_strengths = pd.DataFrame(
        columns=[
            "year",
            "month",
            "tail",
            "threshold",
            "exceedance",
            "event_strength",
            amplitude_column,
        ]
    )
    summary_cols = [
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
    if high_df.empty and low_df.empty:
        return empty_strengths, pd.DataFrame(columns=summary_cols)

    if not high_df.empty:
        high_threshold = _tail_threshold(high_df[amplitude_column].to_numpy(dtype=float), "high")
        high_df["tail"] = "high"
        high_df["amplitude_threshold"] = high_threshold
        high_df["threshold"] = high_threshold
        high_df["exceedance"] = high_df[amplitude_column].to_numpy(dtype=float) - high_threshold
    if not low_df.empty:
        low_threshold = _tail_threshold(low_df[amplitude_column].to_numpy(dtype=float), "low")
        low_df["tail"] = "low"
        low_df["amplitude_threshold"] = low_threshold
        low_df["threshold"] = low_threshold
        low_df["exceedance"] = low_threshold - low_df[amplitude_column].to_numpy(dtype=float)

    strengths_df = pd.concat([high_df, low_df], ignore_index=True)
    strengths_df = strengths_df.sort_values(["year", "month"]).reset_index(drop=True)
    strengths_df["exceedance"] = strengths_df["exceedance"].clip(lower=0.0)
    strengths_df["event_strength"] = strengths_df["exceedance"].to_numpy(dtype=float)

    n_total = int(len(labeled_df))
    rows = []
    for tail, tail_df in (("high", strengths_df[strengths_df["tail"] == "high"]), ("low", strengths_df[strengths_df["tail"] == "low"])):
        rows.extend(
            _build_tail_rows(
                tail_df,
                tail=tail,
                n_total=n_total,
                route="B",
                strength_unit=amplitude_column,
                return_periods=return_periods,
            )
        )

    return (
        strengths_df.loc[
            :, ["year", "month", "tail", "threshold", "exceedance", "event_strength", amplitude_column]
        ].copy(),
        pd.DataFrame(rows, columns=summary_cols),
    )
