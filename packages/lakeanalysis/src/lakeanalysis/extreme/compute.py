"""Shared compute helpers for extreme-event workflows."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.decomposition.series import normalize_monthly_series


def validate_monthly_series(series_df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize one-lake monthly series."""
    return normalize_monthly_series(series_df)


def assign_extreme_labels(
    index_df: pd.DataFrame,
    threshold_low: pd.Series | float,
    threshold_high: pd.Series | float,
) -> pd.DataFrame:
    """Assign extreme-low / normal / extreme-high labels."""
    labeled_df = index_df.copy()
    labeled_df["extreme_label"] = np.select(
        [
            labeled_df["index_value"] <= threshold_low,
            labeled_df["index_value"] >= threshold_high,
        ],
        ["extreme_low", "extreme_high"],
        default="normal",
    )
    return labeled_df


def extract_extreme_events(
    labeled_df: pd.DataFrame,
    value_columns: tuple[str, ...] | None = None,
) -> pd.DataFrame:
    """Extract one row per extreme month."""
    extreme_df = labeled_df.loc[labeled_df["extreme_label"] != "normal"].copy()
    if extreme_df.empty:
        base_cols = [
            "hylak_id",
            "year",
            "month",
            "event_type",
            "water_area",
            "index_value",
            "threshold",
            "extreme_label",
        ]
        if value_columns:
            base_cols.extend(c for c in value_columns if c not in base_cols)
        return pd.DataFrame(columns=base_cols)

    extreme_df["event_type"] = np.where(
        extreme_df["extreme_label"] == "extreme_high",
        "high",
        "low",
    )
    extreme_df["threshold"] = np.where(
        extreme_df["event_type"] == "high",
        extreme_df.get("threshold_high", np.nan),
        extreme_df.get("threshold_low", np.nan),
    )
    base_cols = [
        "hylak_id",
        "year",
        "month",
        "event_type",
        "water_area",
        "index_value",
        "threshold",
        "extreme_label",
    ]
    cols = base_cols[:]
    if value_columns:
        cols.extend(c for c in value_columns if c not in cols)
    return extreme_df.reindex(columns=cols)


def detect_abrupt_transitions(
    labeled_df: pd.DataFrame,
    value_column: str = "index_value",
) -> pd.DataFrame:
    """Detect one-step low-to-high and high-to-low transitions."""
    ordered_df = labeled_df.sort_values(["year", "month"]).reset_index(drop=True)
    next_df = ordered_df.shift(-1)
    adjacency_mask = (next_df["month_ordinal"] - ordered_df["month_ordinal"]) == 1

    low_to_high = (
        (ordered_df["extreme_label"] == "extreme_low")
        & (next_df["extreme_label"] == "extreme_high")
        & adjacency_mask
    )
    high_to_low = (
        (ordered_df["extreme_label"] == "extreme_high")
        & (next_df["extreme_label"] == "extreme_low")
        & adjacency_mask
    )
    transition_mask = low_to_high | high_to_low

    if not transition_mask.any():
        return pd.DataFrame(
            columns=[
                "hylak_id",
                "from_year",
                "from_month",
                "to_year",
                "to_month",
                "transition_type",
                "from_index_value",
                "to_index_value",
                "from_label",
                "to_label",
            ]
        )

    transitions_df = pd.DataFrame(
        {
            "hylak_id": ordered_df.loc[transition_mask, "hylak_id"].to_numpy(),
            "from_year": ordered_df.loc[transition_mask, "year"].to_numpy(dtype=int),
            "from_month": ordered_df.loc[transition_mask, "month"].to_numpy(dtype=int),
            "to_year": next_df.loc[transition_mask, "year"].to_numpy(dtype=int),
            "to_month": next_df.loc[transition_mask, "month"].to_numpy(dtype=int),
            "transition_type": np.where(
                low_to_high.loc[transition_mask].to_numpy(),
                "low_to_high",
                "high_to_low",
            ),
            "from_index_value": ordered_df.loc[transition_mask, value_column].to_numpy(dtype=float),
            "to_index_value": next_df.loc[transition_mask, value_column].to_numpy(dtype=float),
            "from_label": ordered_df.loc[transition_mask, "extreme_label"].to_numpy(),
            "to_label": next_df.loc[transition_mask, "extreme_label"].to_numpy(),
        }
    )
    return transitions_df.reset_index(drop=True)
