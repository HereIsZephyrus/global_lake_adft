"""PWM extreme event extraction with runs declustering and exponential decay
index for Hawkes input."""

from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from lakeanalysis.hawkes.bridge import build_events_from_pwm


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
    warnings.warn(
        "run_runs_declustering is a legacy compatibility helper. "
        "Prefer the S_k + segment pipeline in lakeanalysis.pwm.events.",
        DeprecationWarning,
        stacklevel=2,
    )
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
    phi_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Compute exponential decay strength S_k over the full monthly timeline.

    ``S_k`` is defined as ``sum(phi_i * exp(-a * gap(i, k)))`` over all
    extreme months ``i <= k``. Normal months do not contribute new ``phi``;
    they only decay the accumulated extreme-memory strength.

    Args:
        labeled_df: DataFrame from ``assign_pwm_extreme_labels``,
            containing year, month, index_value, extreme_label,
            threshold_low, threshold_high for ALL months.
        decay_rate: λ parameter controlling decay strength.
            Default 1.0 → e^{-1} per month.
        phi_df: Optional month-level strength table with columns ``year``,
            ``month``, ``phi``. If omitted, raw exceedance is used as the
            fallback ``phi``.

    Returns:
        DataFrame with columns: year, month, S_k, has_high, has_low,
        phi_i, exceedance. One row per month in the original timeline.
    """
    if labeled_df.empty:
        return pd.DataFrame(
            columns=[
                "year",
                "month",
                "S_k",
                "has_high",
                "has_low",
                "phi_i",
                "exceedance",
            ]
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
    df["month_seq"] = df["year"].astype(int) * 12 + df["month"].astype(int)

    high_mask = df["extreme_label"] == "extreme_high"
    low_mask = df["extreme_label"] == "extreme_low"
    extreme_mask = high_mask | low_mask

    df["has_high"] = high_mask.to_numpy()
    df["has_low"] = low_mask.to_numpy()

    exceedance = np.zeros(len(df), dtype=float)
    exceedance[high_mask] = (
        df.loc[high_mask, "index_value"].to_numpy(dtype=float)
        - df.loc[high_mask, "threshold_high"].to_numpy(dtype=float)
    )
    exceedance[low_mask] = (
        df.loc[low_mask, "threshold_low"].to_numpy(dtype=float)
        - df.loc[low_mask, "index_value"].to_numpy(dtype=float)
    )
    exceedance = np.abs(exceedance)

    phi_i = exceedance.copy()
    if phi_df is not None:
        required_phi_cols = {"year", "month", "phi"}
        missing_phi = required_phi_cols - set(phi_df.columns)
        if missing_phi:
            raise ValueError(
                f"phi_df missing required columns: {sorted(missing_phi)}"
            )
        merged = df.loc[:, ["year", "month"]].merge(
            phi_df.loc[:, ["year", "month", "phi"]],
            on=["year", "month"],
            how="left",
        )
        phi_values = merged["phi"].to_numpy(dtype=float)
        phi_i = np.nan_to_num(phi_values, nan=0.0)

    # Only extreme months are allowed to inject new strength.
    phi_i = np.where(extreme_mask.to_numpy(dtype=bool), phi_i, 0.0)

    s_k = np.zeros(len(df), dtype=float)
    running_strength = 0.0
    prev_month_seq: int | None = None
    for idx in range(len(df)):
        month_seq = int(df.iloc[idx]["month_seq"])
        if prev_month_seq is not None:
            month_gap = month_seq - prev_month_seq
            running_strength *= float(np.exp(-decay_rate * month_gap))
        running_strength += float(phi_i[idx])
        s_k[idx] = running_strength
        prev_month_seq = month_seq

    result = pd.DataFrame(
        {
            "year": df["year"].to_numpy(dtype=int),
            "month": df["month"].to_numpy(dtype=int),
            "S_k": s_k,
            "has_high": df["has_high"].to_numpy(),
            "has_low": df["has_low"].to_numpy(),
            "phi_i": phi_i,
            "exceedance": exceedance,
        }
    )
    return result


def extract_segments(decay_df: pd.DataFrame) -> pd.DataFrame:
    """Extract abrupt-transition and unilateral segments from S_k sequence.

    A segment is anchored by extreme months and may include a single normal
    bridge month only when that normal month still satisfies ``S_k > 1``.
    Two consecutive normal months always break the segment.

    A *transition* segment contains at least one high *and* one low extreme.

    A *unilateral* segment contains only one type of extreme event.

    Args:
        decay_df: Output from ``compute_decay_index``. Must contain
            columns: year, month, S_k, has_high, has_low.

    Returns:
        DataFrame with columns: segment_id, start_year, start_month,
        end_year, end_month, duration_months, segment_type, has_high,
        has_low, max_S, mean_S, integral_S, n_extreme_events,
        first_extreme_type, last_extreme_type.
    """
    empty_result = pd.DataFrame(
        columns=[
            "segment_id", "start_year", "start_month",
            "end_year", "end_month", "duration_months",
            "segment_type", "has_high", "has_low",
            "max_S", "mean_S", "integral_S", "n_extreme_events",
            "first_extreme_type", "last_extreme_type",
        ]
    )
    if decay_df.empty:
        return empty_result

    required_cols = {"year", "month", "S_k", "has_high", "has_low"}
    missing = required_cols - set(decay_df.columns)
    if missing:
        raise ValueError(
            f"decay_df missing required columns: {sorted(missing)}"
        )

    df = decay_df.sort_values(["year", "month"]).reset_index(drop=True)

    segments: list[dict] = []
    seg_id = 0
    n = len(df)
    seg_start: int | None = None
    seg_end: int | None = None
    consecutive_normals = 0

    def close_segment(end_idx: int) -> None:
        nonlocal seg_id, seg_start, seg_end, consecutive_normals
        if seg_start is None:
            return
        seg_df = df.iloc[seg_start:end_idx + 1]
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
                "duration_months": len(seg_df),
                "segment_type": seg_type,
                "has_high": seg_has_high,
                "has_low": seg_has_low,
                "max_S": float(seg_df["S_k"].max()),
                "mean_S": float(seg_df["S_k"].mean()),
                "integral_S": float(seg_df["S_k"].sum()),
                "n_extreme_events": n_ext,
                "first_extreme_type": first_extreme_type,
                "last_extreme_type": last_extreme_type,
            }
        )

        seg_start = None
        seg_end = None
        consecutive_normals = 0

    for idx in range(n):
        row = df.iloc[idx]
        is_extreme = bool(row["has_high"] or row["has_low"])
        if is_extreme:
            if seg_start is None:
                seg_start = idx
            seg_end = idx
            consecutive_normals = 0
            continue

        if seg_start is None:
            continue

        consecutive_normals += 1
        if consecutive_normals >= 2 or float(row["S_k"]) <= 1.0:
            close_segment(seg_end if seg_end is not None else idx - 1)
            continue

        seg_end = idx

    if seg_start is not None and seg_end is not None:
        close_segment(seg_end)

    if not segments:
        return empty_result
    return pd.DataFrame(segments)


def extract_hawkes_events_from_segments(
    labeled_df: pd.DataFrame,
    decay_df: pd.DataFrame,
    segments_df: pd.DataFrame,
) -> pd.DataFrame:
    """Extract Hawkes-ready events from transition segments.

    Only transition segments (containing both high and low extremes) are used.
    Within each transition segment, consecutive months of the same extreme type
    are collapsed into one representative event (max severity).

    Args:
        labeled_df: From ``assign_pwm_extreme_labels``; columns include
            year, month, water_area, index_value, threshold_low,
            threshold_high, extreme_label.
        decay_df: From ``compute_decay_index``; aligned with labeled_df,
            contains has_high, has_low boolean columns.
        segments_df: From ``extract_segments``; contains segment boundaries
            and type ("transition" / "unilateral").

    Returns:
        DataFrame with columns: year, month, event_type, water_area,
        index_value, threshold, severity, time.
    """
    transition = segments_df[segments_df["segment_type"] == "transition"]
    if transition.empty:
        return pd.DataFrame(
            columns=[
                "year", "month", "event_type", "water_area",
                "index_value", "threshold", "severity", "time",
            ]
        )

    # Build year_month keys for efficient segment filtering
    labeled_df = labeled_df.copy()
    labeled_df["year_month"] = labeled_df["year"] * 100 + labeled_df["month"]
    decay_df = decay_df.copy()
    decay_df["year_month"] = decay_df["year"] * 100 + decay_df["month"]

    events: list[dict] = []
    for _, seg in transition.iterrows():
        start_ym = int(seg["start_year"]) * 100 + int(seg["start_month"])
        end_ym = int(seg["end_year"]) * 100 + int(seg["end_month"])

        seg_mask = (decay_df["year_month"] >= start_ym) & (decay_df["year_month"] <= end_ym)
        seg_decay = decay_df[seg_mask]
        extreme_mask = seg_decay["has_high"] | seg_decay["has_low"]
        if not extreme_mask.any():
            continue

        extreme_yms = set(seg_decay.loc[extreme_mask, "year_month"].to_numpy(dtype=int))
        seg_labeled = labeled_df[labeled_df["year_month"].isin(extreme_yms)].sort_values("year_month")

        # Determine event type
        seg_labeled["event_type"] = np.where(
            seg_labeled["extreme_label"] == "extreme_high", "high", "low"
        )

        # Segment-scoped runs declustering: consecutive same-type → one event
        current_cluster: list[pd.Series] = []
        for _, row in seg_labeled.iterrows():
            if not current_cluster:
                current_cluster.append(row)
            elif row["event_type"] == current_cluster[-1]["event_type"]:
                current_cluster.append(row)
            else:
                _emit_cluster(events, current_cluster)
                current_cluster = [row]
        if current_cluster:
            _emit_cluster(events, current_cluster)

    if not events:
        return pd.DataFrame(
            columns=[
                "year", "month", "event_type", "water_area",
                "index_value", "threshold", "severity", "time",
            ]
        )

    result = pd.DataFrame(events)
    return result.sort_values("year_month").reset_index(drop=True)


def _emit_cluster(events: list[dict], cluster: list[pd.Series]) -> None:
    """Pick the representative (max severity) from a same-type cluster."""
    best = max(
        cluster,
        key=lambda r: abs(float(r["index_value"]) - (
            float(r["threshold_high"]) if r["extreme_label"] == "extreme_high"
            else float(r["threshold_low"])
        )),
    )
    event_type = "high" if best["extreme_label"] == "extreme_high" else "low"
    threshold = (
        best["threshold_high"] if event_type == "high" else best["threshold_low"]
    )
    severity = abs(float(best["index_value"]) - float(threshold))
    events.append({
        "year": int(best["year"]),
        "month": int(best["month"]),
        "year_month": int(best["year"]) * 100 + int(best["month"]),
        "event_type": event_type,
        "water_area": float(best["water_area"]),
        "index_value": float(best["index_value"]),
        "threshold": float(threshold),
        "severity": float(severity),
        "time": float(best["year"]) + (float(best["month"]) - 1.0) / 12.0,
    })


build_hawkes_event_series_from_pwm_events = build_events_from_pwm
