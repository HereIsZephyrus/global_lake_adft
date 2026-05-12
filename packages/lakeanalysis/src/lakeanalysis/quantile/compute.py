"""Pure compute helpers for monthly anomaly transition detection.

These functions consume a :class:`~lakeanalysis.decomposition.DecompositionResult`
produced by any decomposition method and are agnostic to how ``index_value``
was computed.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeanalysis.decomposition.base import DecompositionResult
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
        df = df.drop_duplicates(subset=["year", "month"], keep="first")

    df = df.sort_values(["year", "month"]).reset_index(drop=True)
    df["year_month_key"] = df["year"] * 100 + df["month"]
    df["month_ordinal"] = df["year"] * 12 + (df["month"] - 1)
    return df


def compute_anomaly_thresholds(
    index_df: pd.DataFrame,
    q_low_pct: float = 10.0,
    q_high_pct: float = 90.0,
) -> tuple[float, float]:
    """Compute global quantile thresholds on ``index_value``.

    Args:
        index_df: Must contain column ``index_value``.
        q_low_pct: Lower quantile percentile (default 10).
        q_high_pct: Upper quantile percentile (default 90).

    Returns:
        (q_low, q_high) threshold tuple.
    """
    values = index_df["index_value"].to_numpy(dtype=float)
    q_low, q_high = np.quantile(values, [q_low_pct / 100, q_high_pct / 100], method="linear")
    return float(q_low), float(q_high)


def assign_extreme_labels(
    index_df: pd.DataFrame,
    q_low: float,
    q_high: float,
) -> pd.DataFrame:
    """Assign extreme-low / normal / extreme-high labels."""
    labeled_df = index_df.copy()
    labeled_df["q_low"] = q_low
    labeled_df["q_high"] = q_high
    labeled_df["extreme_label"] = np.select(
        [
            labeled_df["index_value"] <= q_low,
            labeled_df["index_value"] >= q_high,
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
                "index_value",
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
        "index_value",
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
            "from_anomaly": ordered_df.loc[transition_mask, "index_value"].to_numpy(dtype=float),
            "to_anomaly": next_df.loc[transition_mask, "index_value"].to_numpy(dtype=float),
            "from_label": ordered_df.loc[transition_mask, "extreme_label"].to_numpy(),
            "to_label": next_df.loc[transition_mask, "extreme_label"].to_numpy(),
        }
    )
    return transitions_df.reset_index(drop=True)


def run_monthly_anomaly_transition(
    result: DecompositionResult,
    *,
    hylak_id: int | None = None,
    min_valid_per_month: int | None = 20,
    min_valid_observations: int | None = 240,
) -> QuantileResult:
    """Run the one-lake anomaly labelling workflow.

    Args:
        result: Decomposition result with ``index_df`` containing ``index_value``.
        hylak_id: Optional lake identifier.
        min_valid_per_month: Minimum observations per calendar month (None = skip).
        min_valid_observations: Minimum total observations (None = skip).
    """
    labels_df = result.index_df

    month_counts = labels_df.groupby("month").size().reindex(range(1, 13), fill_value=0)
    if min_valid_per_month is not None and (month_counts < min_valid_per_month).any():
        raise ValueError("Insufficient valid observations for one or more calendar months")
    if min_valid_observations is not None and len(labels_df) < min_valid_observations:
        raise ValueError("Insufficient valid monthly observations overall")

    q_low, q_high = compute_anomaly_thresholds(labels_df)
    labeled_df = assign_extreme_labels(labels_df, q_low=q_low, q_high=q_high)

    # backward-compat aliases for store layer
    labeled_df["anomaly"] = labeled_df["index_value"]
    labeled_df["monthly_climatology"] = labeled_df["index_value"]

    if hylak_id is not None:
        labeled_df.insert(0, "hylak_id", hylak_id)
    else:
        labeled_df.insert(0, "hylak_id", pd.Series([pd.NA] * len(labeled_df), dtype="Int64"))

    extremes_df = extract_extreme_events(labeled_df)
    transitions_df = detect_abrupt_transitions(labeled_df)

    return QuantileResult(
        hylak_id=hylak_id,
        climatology_df=pd.DataFrame({"month": range(1, 13), "hylak_id": [hylak_id] * 12}),
        labels_df=labeled_df,
        extremes_df=extremes_df,
        transitions_df=transitions_df,
        q_low=q_low,
        q_high=q_high,
    )
