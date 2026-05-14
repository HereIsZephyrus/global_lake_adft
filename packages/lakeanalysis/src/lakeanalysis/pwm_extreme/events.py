"""PWM extreme event extraction with runs declustering and exponential decay
index for Hawkes input."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from lakeanalysis.hawkes.bridge import build_events_from_pwm


@dataclass(frozen=True)
class DecaySegments:
    decay_df: pd.DataFrame
    segments_df: pd.DataFrame


def run_runs_declustering(
    events_df: pd.DataFrame,
    *,
    run_length: int = 1,
) -> pd.DataFrame:
    """Apply runs declustering to PWM extreme events.

    Consecutive events of the same type are merged into one cluster when
    the gap in months between them is at most ``run_length``.  The
    representative event is the one with the largest severity within the
    cluster.

    Args:
        events_df: Output from ``extract_pwm_extreme_events``, sorted by
            year/month.  Must contain columns: year, month, event_type,
            water_area, threshold, severity.
        run_length: Minimum number of normal months required between two
            events of the same type for them to belong to separate
            clusters.

    Returns:
        DataFrame with the same columns as input, plus ``cluster_id`` and
        ``cluster_size``, containing one representative row per cluster.
    """
    if events_df.empty:
        return pd.DataFrame(
            columns=list(events_df.columns) + ["cluster_id", "cluster_size", "time"]
        )

    df = events_df.sort_values(["year", "month"]).reset_index(drop=True)
    df["time"] = df["year"] + (df["month"] - 1) / 12.0
    df["month_seq"] = df["year"] * 12 + df["month"]

    clusters_records: list[list[dict]] = []
    current_cluster: list[dict] = []

    for _, row in df.iterrows():
        if not current_cluster:
            current_cluster.append(row.to_dict())
        elif (
            row["event_type"] == current_cluster[-1]["event_type"]
            and row["month_seq"] - current_cluster[-1]["month_seq"] - 1 < run_length
        ):
            current_cluster.append(row.to_dict())
        else:
            clusters_records.append(current_cluster)
            current_cluster = [row.to_dict()]

    if current_cluster:
        clusters_records.append(current_cluster)

    records: list[dict] = []
    for cluster_id, cluster in enumerate(clusters_records, start=1):
        representative = max(cluster, key=lambda r: r.get("severity", 0.0))
        records.append(
            {
                "year": int(representative["year"]),
                "month": int(representative["month"]),
                "event_type": representative["event_type"],
                "water_area": float(representative["water_area"]),
                "threshold": float(representative["threshold"]),
                "severity": float(representative["severity"]),
                "cluster_id": cluster_id,
                "cluster_size": len(cluster),
                "time": float(representative["time"]),
            }
        )

    if not records:
        return pd.DataFrame(
            columns=list(events_df.columns) + ["cluster_id", "cluster_size", "time"]
        )

    return pd.DataFrame(records)


build_hawkes_event_series_from_pwm_events = build_events_from_pwm


def compute_decay_index(
    labeled_df: pd.DataFrame,
    *,
    decay_rate: float = 1.0,
) -> pd.DataFrame:
    """Compute exponential decay index C_k over the full monthly timeline.

    This function replaces hard-threshold runs declustering with a
    continuous exponential decay model.  It operates on ``index_value``
    (from STL decomposition) across all months, including normal ones.

    Algorithm (per lake, already filtered upstream):
    1. Per-type z-score of severity = |index_value - threshold|
    2. f_i = ln(|z_i - 0.5| + 1) for extreme months (0 for normal)
    3. Step through timeline: C -= e^{-λ} × C, C += f_i

    Args:
        labeled_df: DataFrame from ``assign_pwm_extreme_labels``,
            containing year, month, index_value, extreme_label,
            threshold_low, threshold_high for ALL months.
        decay_rate: λ parameter controlling decay strength.
            Default 1.0 → e^{-1} per month.

    Returns:
        DataFrame with columns: year, month, C_k, has_high, has_low,
        f_i, z_i.  One row per month in the original timeline.
    """
    if labeled_df.empty:
        return pd.DataFrame(
            columns=["year", "month", "C_k", "has_high", "has_low", "f_i", "z_i"]
        )

    required_cols = {"year", "month", "index_value", "extreme_label",
                     "threshold_low", "threshold_high"}
    missing = required_cols - set(labeled_df.columns)
    if missing:
        raise ValueError(
            f"labeled_df missing required columns: {sorted(missing)}. "
            "Use assign_pwm_extreme_labels (STL decomposition path) instead of "
            "the deprecated raw water_area path."
        )

    df = labeled_df.sort_values(["year", "month"]).reset_index(drop=True)

    high_mask = df["extreme_label"] == "extreme_high"
    low_mask = df["extreme_label"] == "extreme_low"

    df["has_high"] = high_mask.to_numpy()
    df["has_low"] = low_mask.to_numpy()

    severity = np.full(len(df), np.nan, dtype=float)
    severity[high_mask] = (
        df.loc[high_mask, "index_value"].to_numpy(dtype=float)
        - df.loc[high_mask, "threshold_high"].to_numpy(dtype=float)
    )
    severity[low_mask] = (
        df.loc[low_mask, "threshold_low"].to_numpy(dtype=float)
        - df.loc[low_mask, "index_value"].to_numpy(dtype=float)
    )
    severity = np.abs(severity)

    z_scores = np.zeros(len(df), dtype=float)
    for mask, label in ((high_mask, "high"), (low_mask, "low")):
        if not mask.any():
            continue
        s_vals = severity[mask]
        mu = float(np.mean(s_vals))
        sigma = float(np.std(s_vals))
        if sigma < 1e-15:
            continue
        z_scores[mask] = (s_vals - mu) / sigma

    extreme_mask = high_mask | low_mask
    f_i = np.zeros(len(df), dtype=float)
    f_i[extreme_mask] = np.log(np.abs(z_scores[extreme_mask] - 0.5) + 1.0)

    decay_factor = float(np.exp(-decay_rate))
    c_k = np.zeros(len(df), dtype=float)
    c = 0.0
    for idx in range(len(df)):
        c *= decay_factor
        c += float(f_i[idx])
        c_k[idx] = c

    result = pd.DataFrame(
        {
            "year": df["year"].to_numpy(dtype=int),
            "month": df["month"].to_numpy(dtype=int),
            "C_k": c_k,
            "has_high": df["has_high"].to_numpy(),
            "has_low": df["has_low"].to_numpy(),
            "f_i": f_i,
            "z_i": z_scores,
        }
    )
    return result


def extract_segments(decay_df: pd.DataFrame) -> pd.DataFrame:
    """Extract abrupt-transition and unilateral segments from C_k sequence.

    A *transition* segment is a continuous run of months with C_k > 0
    that contains at least one high *and* one low extreme event.

    A *unilateral* segment contains only one type of extreme event.

    Args:
        decay_df: Output from ``compute_decay_index``.  Must contain
            columns: year, month, C_k, has_high, has_low.

    Returns:
        DataFrame with columns: segment_id, start_year, start_month,
        end_year, end_month, duration_months, segment_type, has_high,
        has_low, max_C, mean_C, integral_C, n_extreme_events,
        first_extreme_type, last_extreme_type.
    """
    empty_result = pd.DataFrame(
        columns=[
            "segment_id", "start_year", "start_month",
            "end_year", "end_month", "duration_months",
            "segment_type", "has_high", "has_low",
            "max_C", "mean_C", "integral_C", "n_extreme_events",
            "first_extreme_type", "last_extreme_type",
        ]
    )
    if decay_df.empty:
        return empty_result

    required_cols = {"year", "month", "C_k", "has_high", "has_low"}
    missing = required_cols - set(decay_df.columns)
    if missing:
        raise ValueError(
            f"decay_df missing required columns: {sorted(missing)}"
        )

    df = decay_df.sort_values(["year", "month"]).reset_index(drop=True)
    active = (df["C_k"] > 0.0).to_numpy(dtype=bool)

    segments: list[dict] = []
    seg_id = 0
    i = 0
    n = len(df)

    while i < n:
        if not active[i]:
            i += 1
            continue

        j = i
        while j < n and active[j]:
            j += 1

        seg_df = df.iloc[i:j]
        seg_has_high = bool(seg_df["has_high"].any())
        seg_has_low = bool(seg_df["has_low"].any())
        seg_type = "transition" if (seg_has_high and seg_has_low) else "unilateral"

        extreme_in_seg = seg_df["has_high"] | seg_df["has_low"]
        n_ext = int(extreme_in_seg.sum())

        first_extreme_type = None
        last_extreme_type = None
        if n_ext > 0:
            extreme_rows = seg_df.loc[extreme_in_seg]
            first_extreme_type = (
                "high" if bool(extreme_rows.iloc[0]["has_high"]) else "low"
            )
            last_extreme_type = (
                "high" if bool(extreme_rows.iloc[-1]["has_high"]) else "low"
            )

        seg_id += 1
        segments.append(
            {
                "segment_id": seg_id,
                "start_year": int(seg_df.iloc[0]["year"]),
                "start_month": int(seg_df.iloc[0]["month"]),
                "end_year": int(seg_df.iloc[-1]["year"]),
                "end_month": int(seg_df.iloc[-1]["month"]),
                "duration_months": j - i,
                "segment_type": seg_type,
                "has_high": seg_has_high,
                "has_low": seg_has_low,
                "max_C": float(seg_df["C_k"].max()),
                "mean_C": float(seg_df["C_k"].mean()),
                "integral_C": float(seg_df["C_k"].sum()),
                "n_extreme_events": n_ext,
                "first_extreme_type": first_extreme_type,
                "last_extreme_type": last_extreme_type,
            }
        )
        i = j

    if not segments:
        return empty_result
    return pd.DataFrame(segments)
