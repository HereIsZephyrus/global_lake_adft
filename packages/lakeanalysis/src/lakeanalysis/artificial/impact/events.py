"""Z-score anomaly event detection for lake water_area time series."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


def detect_zscore_events(
    df: pd.DataFrame,
    threshold: float = 3.0,
) -> list[dict]:
    """Detect months where |z-score| exceeds threshold.

    Args:
        df: DataFrame with columns [year, month, water_area].
        threshold: Z-score absolute threshold (default 3.0).

    Returns:
        List of dicts with keys: year, month, water_area, z_score.
    """
    if df.empty or len(df) < 2:
        return []
    series = df["water_area"].to_numpy(dtype=float)
    mean = float(np.mean(series))
    std = float(np.std(series))
    if std == 0:
        return []

    events = []
    for _, row in df.iterrows():
        z = (float(row["water_area"]) - mean) / std
        if abs(z) > threshold:
            events.append({
                "year": int(row["year"]),
                "month": int(row["month"]),
                "water_area": float(row["water_area"]),
                "z_score": round(z, 3),
            })
    return events


def compute_event_stats(
    df: pd.DataFrame,
    threshold: float = 3.0,
) -> dict:
    """Compute Z-score anomaly event statistics for a single lake.

    Args:
        df: DataFrame with columns [year, month, water_area].
        threshold: Z-score absolute threshold (default 3.0).

    Returns:
        Dict with keys: n_events, n_obs, anomaly_ratio, events.
    """
    events = detect_zscore_events(df, threshold=threshold)
    n_obs = len(df)
    return {
        "n_events": len(events),
        "n_obs": n_obs,
        "anomaly_ratio": len(events) / n_obs if n_obs > 0 else np.nan,
        "events": events,
    }


def compute_pair_events(
    df_artificial: pd.DataFrame,
    df_natural: pd.DataFrame,
    threshold: float = 3.0,
) -> dict:
    """Compute Z-score event statistics for an artificial-natural lake pair.

    Includes the count of events unique to the artificial lake (anomalous in
    the artificial lake but the same month is not anomalous in the natural
    lake).

    Args:
        df_artificial: DataFrame with columns [year, month, water_area].
        df_natural: Same for the paired natural lake.
        threshold: Z-score absolute threshold (default 3.0).

    Returns:
        Dict with keys:
          - n_events_a, n_obs_a, anomaly_ratio_a  (artificial)
          - n_events_n, n_obs_n, anomaly_ratio_n  (natural)
          - n_unique_a  (artificial-only events)
          - delta_anomaly_ratio
    """
    stats_a = compute_event_stats(df_artificial, threshold=threshold)
    stats_n = compute_event_stats(df_natural, threshold=threshold)

    natural_anomaly_ym = set()
    for ev in stats_n["events"]:
        natural_anomaly_ym.add((ev["year"], ev["month"]))

    n_unique_a = 0
    for ev in stats_a["events"]:
        if (ev["year"], ev["month"]) not in natural_anomaly_ym:
            n_unique_a += 1

    def _delta(a: float, n: float) -> float:
        if np.isnan(a) or np.isnan(n):
            return np.nan
        return a - n

    return {
        "n_events_a": stats_a["n_events"],
        "n_obs_a": stats_a["n_obs"],
        "anomaly_ratio_a": stats_a["anomaly_ratio"],
        "n_events_n": stats_n["n_events"],
        "n_obs_n": stats_n["n_obs"],
        "anomaly_ratio_n": stats_n["anomaly_ratio"],
        "n_unique_a": n_unique_a,
        "delta_anomaly_ratio": _delta(stats_a["anomaly_ratio"], stats_n["anomaly_ratio"]),
    }
