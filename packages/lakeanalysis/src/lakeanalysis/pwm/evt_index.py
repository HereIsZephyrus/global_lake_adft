"""Route A EVT helpers on ``index_value`` exceedances.

This file is a **method adapter** — it converts PWM-labelled data into
exceedance samples for the shared EVT algorithm layer in ``evt_common.py``.
It does **not** implement GPD fitting or return-level math directly.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.extreme.evt import (
    DEFAULT_RETURN_PERIODS,
    EVT_STRENGTH_COLS,
    EVT_SUMMARY_COLS,
    ROUTE_A,
    build_fitted_tail_summary_rows,
)


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
    empty_strengths = pd.DataFrame(columns=EVT_STRENGTH_COLS)
    if extremes_df.empty:
        return empty_strengths, pd.DataFrame(columns=EVT_SUMMARY_COLS)

    extremes_df = extremes_df.sort_values(["year", "month"]).reset_index(drop=True)
    extremes_df["event_strength"] = extremes_df["exceedance"].to_numpy(dtype=float)

    n_total = int(len(labeled_df))
    summary_rows = []
    for tail in ("high", "low"):
        tail_df = extremes_df[extremes_df["tail"] == tail]
        summary_rows.extend(
            build_fitted_tail_summary_rows(
                tail_df,
                tail=tail,
                n_total=n_total,
                return_periods=return_periods,
                evt_route=ROUTE_A,
                strength_unit="index_value",
            )
        )

    strengths_df = extremes_df.loc[:, EVT_STRENGTH_COLS].copy()
    summary_df = pd.DataFrame(summary_rows, columns=EVT_SUMMARY_COLS)
    return strengths_df, summary_df
