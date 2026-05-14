"""Route A EVT helpers on ``index_value`` exceedances."""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import genpareto

DEFAULT_RETURN_PERIODS = (2, 5, 10, 20)


def _exceedance_from_labels(labeled_df: pd.DataFrame) -> pd.DataFrame:
    required_cols = {
        "year",
        "month",
        "index_value",
        "extreme_label",
        "threshold_low",
        "threshold_high",
    }
    missing = required_cols - set(labeled_df.columns)
    if missing:
        raise ValueError(f"labeled_df missing required columns: {sorted(missing)}")

    df = labeled_df.copy()
    high_mask = df["extreme_label"] == "extreme_high"
    low_mask = df["extreme_label"] == "extreme_low"

    df["tail"] = np.select([high_mask, low_mask], ["high", "low"], default="normal")
    df["threshold"] = np.where(
        high_mask,
        df["threshold_high"],
        np.where(low_mask, df["threshold_low"], np.nan),
    )
    df["exceedance"] = 0.0
    df.loc[high_mask, "exceedance"] = (
        df.loc[high_mask, "index_value"] - df.loc[high_mask, "threshold_high"]
    )
    df.loc[low_mask, "exceedance"] = (
        df.loc[low_mask, "threshold_low"] - df.loc[low_mask, "index_value"]
    )
    return df.loc[high_mask | low_mask].copy()


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


def _build_tail_rows(
    tail_df: pd.DataFrame,
    *,
    tail: str,
    n_total: int,
    return_periods: tuple[int, ...],
) -> list[dict]:
    threshold = float(tail_df["threshold"].iloc[0]) if not tail_df.empty else np.nan
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
                "evt_route": "A",
                "strength_unit": "index_value",
            }
            for period in return_periods
        ]

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
                "evt_route": "A",
                "strength_unit": "index_value",
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
                "evt_route": "A",
                "strength_unit": "index_value",
            }
            for period in return_periods
        ]


def compute_evt_index_strengths(
    labeled_df: pd.DataFrame,
    *,
    return_periods: tuple[int, ...] = DEFAULT_RETURN_PERIODS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute Route A month-level strengths and tail fit summaries.

    The current minimal implementation uses exceedance as the primary event
    strength and fits a GPD only for diagnostics / future upgrade paths.
    Fit failure falls back to raw exceedance automatically because
    ``event_strength`` is always set to ``exceedance``.
    """
    extremes_df = _exceedance_from_labels(labeled_df)
    empty_strengths = pd.DataFrame(
        columns=[
            "year",
            "month",
            "tail",
            "threshold",
            "exceedance",
            "event_strength",
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
    if extremes_df.empty:
        return empty_strengths, pd.DataFrame(columns=summary_cols)

    extremes_df = extremes_df.sort_values(["year", "month"]).reset_index(drop=True)
    extremes_df["event_strength"] = extremes_df["exceedance"].to_numpy(dtype=float)

    n_total = int(len(labeled_df))
    summary_rows = []
    for tail in ("high", "low"):
        tail_df = extremes_df[extremes_df["tail"] == tail]
        summary_rows.extend(
            _build_tail_rows(
                tail_df,
                tail=tail,
                n_total=n_total,
                return_periods=return_periods,
            )
        )

    strengths_df = extremes_df.loc[
        :, ["year", "month", "tail", "threshold", "exceedance", "event_strength"]
    ].copy()
    summary_df = pd.DataFrame(summary_rows, columns=summary_cols)
    return strengths_df, summary_df
