"""Route B EVT helpers on continuous amplitude space.

This file is a **method adapter** — it maps PWM percentile thresholds back
to each calendar month's amplitude distribution and constructs exceedance
samples for the shared EVT algorithm layer in ``evt_common.py``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.extreme.evt import (
    DEFAULT_RETURN_PERIODS,
    EVT_STRENGTH_COLS,
    EVT_SUMMARY_COLS,
    ROUTE_B,
    build_fitted_tail_summary_rows,
)


def _tail_threshold(values: np.ndarray, tail: str) -> float:
    if tail == "high":
        return float(np.min(values))
    return float(np.max(values))


def _percentile_to_amplitude_threshold(
    month_df: pd.DataFrame,
    *,
    tail: str,
    amplitude_column: str,
) -> float:
    """Map a PWM percentile threshold back to the month's amplitude space."""
    amplitudes = np.sort(month_df[amplitude_column].to_numpy(dtype=float))
    if len(amplitudes) == 0:
        raise ValueError("month_df must contain at least one observation")
    percentile_threshold = float(
        month_df["threshold_high"].iloc[0] if tail == "high" else month_df["threshold_low"].iloc[0]
    )
    quantile = float(np.clip(percentile_threshold / 100.0, 0.0, 1.0))
    return float(np.quantile(amplitudes, quantile, method="linear"))


def compute_evt_amplitude_strengths(
    labeled_df: pd.DataFrame,
    *,
    amplitude_column: str = "stl_residual",
    return_periods: tuple[int, ...] = DEFAULT_RETURN_PERIODS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute Route B strengths using a continuous amplitude coordinate.

    The amplitude threshold is derived by mapping the pooled PWM percentile
    threshold back to each calendar month's historical amplitude distribution.
    Strength remains the raw amplitude exceedance, while GPD fit and return
    levels are emitted as diagnostics / persisted outputs.
    """
    required_cols = {
        "year",
        "month",
        "extreme_label",
        "threshold_low",
        "threshold_high",
        amplitude_column,
    }
    missing = required_cols - set(labeled_df.columns)
    if missing:
        raise ValueError(f"labeled_df missing required columns: {sorted(missing)}")

    df = labeled_df.copy()
    empty_cols = list(EVT_STRENGTH_COLS) + [amplitude_column]
    empty_strengths = pd.DataFrame(columns=empty_cols)
    extreme_df = df[df["extreme_label"] != "normal"].copy()
    if extreme_df.empty:
        return empty_strengths, pd.DataFrame(columns=EVT_SUMMARY_COLS)

    threshold_rows: list[dict] = []
    for month, month_df in df.groupby("month", sort=True):
        threshold_rows.append(
            {
                "month": int(month),
                "amplitude_threshold_high": _percentile_to_amplitude_threshold(
                    month_df,
                    tail="high",
                    amplitude_column=amplitude_column,
                ),
                "amplitude_threshold_low": _percentile_to_amplitude_threshold(
                    month_df,
                    tail="low",
                    amplitude_column=amplitude_column,
                ),
            }
        )

    threshold_df = pd.DataFrame(threshold_rows)
    strengths_df = extreme_df.merge(threshold_df, on="month", how="left")
    high_mask = strengths_df["extreme_label"] == "extreme_high"
    low_mask = strengths_df["extreme_label"] == "extreme_low"
    strengths_df["tail"] = np.where(high_mask, "high", "low")
    strengths_df["amplitude_threshold"] = np.where(
        high_mask,
        strengths_df["amplitude_threshold_high"],
        strengths_df["amplitude_threshold_low"],
    )
    strengths_df["threshold"] = strengths_df["amplitude_threshold"]
    strengths_df["exceedance"] = 0.0
    strengths_df.loc[high_mask, "exceedance"] = (
        strengths_df.loc[high_mask, amplitude_column].to_numpy(dtype=float)
        - strengths_df.loc[high_mask, "amplitude_threshold"].to_numpy(dtype=float)
    )
    strengths_df.loc[low_mask, "exceedance"] = (
        strengths_df.loc[low_mask, "amplitude_threshold"].to_numpy(dtype=float)
        - strengths_df.loc[low_mask, amplitude_column].to_numpy(dtype=float)
    )
    strengths_df = strengths_df.sort_values(["year", "month"]).reset_index(drop=True)
    strengths_df["exceedance"] = strengths_df["exceedance"].clip(lower=0.0)
    strengths_df["event_strength"] = strengths_df["exceedance"].to_numpy(dtype=float)

    n_total = int(len(labeled_df))
    rows = []
    for tail, tail_df in (
        ("high", strengths_df[strengths_df["tail"] == "high"]),
        ("low", strengths_df[strengths_df["tail"] == "low"]),
    ):
        rows.extend(
            build_fitted_tail_summary_rows(
                tail_df,
                tail=tail,
                n_total=n_total,
                return_periods=return_periods,
                evt_route=ROUTE_B,
                strength_unit=amplitude_column,
            )
        )

    out_cols = list(EVT_STRENGTH_COLS) + [amplitude_column]
    return (
        strengths_df.loc[:, out_cols].copy(),
        pd.DataFrame(rows, columns=EVT_SUMMARY_COLS),
    )
