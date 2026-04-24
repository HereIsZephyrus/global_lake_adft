"""Pure compute helpers for monthly anomaly transition detection."""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.quality.frozen import filter_frozen_rows
from lakesource.quantile.schema import QuantileResult

REQUIRED_COLUMNS = ("year", "month", "water_area")


def validate_monthly_series(series_df: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize one-lake monthly series."""
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in series_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    df = series_df.loc[:, REQUIRED_COLUMNS].copy()
    df["year"] = pd.to_numeric(df["year"], errors="raise").astype(int)
    df["month"] = pd.to_numeric(df["month"], errors="raise").astype(int)
    df["water_area"] = pd.to_numeric(df["water_area"], errors="raise").astype(float)

    if ((df["month"] < 1) | (df["month"] > 12)).any():
        raise ValueError("month must be in 1..12")

    if df.duplicated(["year", "month"]).any():
        raise ValueError("Duplicate year/month observations are not allowed")

    df = df.sort_values(["year", "month"]).reset_index(drop=True)
    df["year_month_key"] = df["year"] * 100 + df["month"]
    df["month_ordinal"] = df["year"] * 12 + (df["month"] - 1)
    return df


def compute_monthly_climatology(valid_df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly climatology for one lake."""
    climatology_df = (
        valid_df.groupby("month", as_index=False)["water_area"]
        .mean()
        .rename(columns={"water_area": "monthly_climatology"})
    )
    return climatology_df.sort_values("month").reset_index(drop=True)


def compute_monthly_anomalies(
    series_df: pd.DataFrame,
    frozen_year_months: set[int] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compute filtered monthly anomalies and the corresponding climatology."""
    valid_df = validate_monthly_series(series_df)
    if frozen_year_months:
        valid_df = filter_frozen_rows(valid_df, frozen_year_months)

    if valid_df.empty:
        raise ValueError("No valid observations remain after filtering")

    climatology_df = compute_monthly_climatology(valid_df)
    labels_df = valid_df.merge(climatology_df, on="month", how="left", validate="many_to_one")
    labels_df["anomaly"] = labels_df["water_area"] - labels_df["monthly_climatology"]
    return labels_df.reset_index(drop=True), climatology_df


def compute_anomaly_thresholds(labels_df: pd.DataFrame) -> tuple[float, float]:
    """Compute lake-relative anomaly thresholds with a fixed quantile rule."""
    anomalies = labels_df["anomaly"].to_numpy(dtype=float)
    q_low, q_high = np.quantile(anomalies, [0.10, 0.90], method="linear")
    return float(q_low), float(q_high)


def assign_extreme_labels(
    labels_df: pd.DataFrame,
    q_low: float,
    q_high: float,
) -> pd.DataFrame:
    """Assign extreme-low, normal, and extreme-high labels."""
    labeled_df = labels_df.copy()
    labeled_df["q_low"] = q_low
    labeled_df["q_high"] = q_high
    labeled_df["extreme_label"] = np.select(
        [
            labeled_df["anomaly"] <= q_low,
            labeled_df["anomaly"] >= q_high,
        ],
        ["extreme_low", "extreme_high"],
        default="normal",
    )
    return labeled_df


def extract_extreme_events(labeled_df: pd.DataFrame) -> pd.DataFrame:
    """Extract one row per extreme month."""
    extreme_df = labeled_df.loc[labeled_df["extreme_label"] != "normal"].copy()
    if extreme_df.empty:
        return pd.DataFrame(
            columns=[
                "hylak_id",
                "year",
                "month",
                "event_type",
                "water_area",
                "monthly_climatology",
                "anomaly",
                "threshold",
            ]
        )

    extreme_df["event_type"] = np.where(
        extreme_df["extreme_label"] == "extreme_high",
        "high",
        "low",
    )
    extreme_df["threshold"] = np.where(
        extreme_df["event_type"] == "high",
        extreme_df["q_high"],
        extreme_df["q_low"],
    )
    columns = [
        "hylak_id",
        "year",
        "month",
        "event_type",
        "water_area",
        "monthly_climatology",
        "anomaly",
        "threshold",
    ]
    return extreme_df.reindex(columns=columns)


def detect_abrupt_transitions(labeled_df: pd.DataFrame) -> pd.DataFrame:
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
                "from_anomaly",
                "to_anomaly",
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
            "from_anomaly": ordered_df.loc[transition_mask, "anomaly"].to_numpy(dtype=float),
            "to_anomaly": next_df.loc[transition_mask, "anomaly"].to_numpy(dtype=float),
            "from_label": ordered_df.loc[transition_mask, "extreme_label"].to_numpy(),
            "to_label": next_df.loc[transition_mask, "extreme_label"].to_numpy(),
        }
    )
    return transitions_df.reset_index(drop=True)


def run_monthly_anomaly_transition(
    series_df: pd.DataFrame,
    *,
    hylak_id: int | None = None,
    frozen_year_months: set[int] | None = None,
    min_valid_per_month: int | None = 20,
    min_valid_observations: int | None = 240,
) -> QuantileResult:
    """Run the one-lake workflow end to end."""
    labels_df, climatology_df = compute_monthly_anomalies(
        series_df,
        frozen_year_months=frozen_year_months,
    )

    month_counts = labels_df.groupby("month").size().reindex(range(1, 13), fill_value=0)
    if min_valid_per_month is not None and (month_counts < min_valid_per_month).any():
        raise ValueError("Insufficient valid observations for one or more calendar months")
    if min_valid_observations is not None and len(labels_df) < min_valid_observations:
        raise ValueError("Insufficient valid monthly observations overall")

    q_low, q_high = compute_anomaly_thresholds(labels_df)
    labeled_df = assign_extreme_labels(labels_df, q_low=q_low, q_high=q_high)
    if hylak_id is not None:
        labeled_df.insert(0, "hylak_id", hylak_id)
        climatology_df.insert(0, "hylak_id", hylak_id)
    else:
        labeled_df.insert(0, "hylak_id", pd.Series([pd.NA] * len(labeled_df), dtype="Int64"))
        climatology_df.insert(
            0, "hylak_id", pd.Series([pd.NA] * len(climatology_df), dtype="Int64")
        )

    extremes_df = extract_extreme_events(labeled_df)
    transitions_df = detect_abrupt_transitions(labeled_df)
    return QuantileResult(
        hylak_id=hylak_id,
        climatology_df=climatology_df,
        labels_df=labeled_df,
        extremes_df=extremes_df,
        transitions_df=transitions_df,
        q_low=q_low,
        q_high=q_high,
    )
