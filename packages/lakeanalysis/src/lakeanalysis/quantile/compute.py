from __future__ import annotations

import numpy as np
import pandas as pd

from lakesource.quantile.schema import QuantileResult

from lakeanalysis.decomposition.base import DecompositionResult
from lakeanalysis.extreme.compute import (
    assign_extreme_labels,
    detect_abrupt_transitions,
    extract_extreme_events,
)
from lakeanalysis.extreme.models import ExtremeResult, QuantileDiagnostics


def compute_anomaly_thresholds(
    index_df: pd.DataFrame,
    q_low_pct: float = 10.0,
    q_high_pct: float = 90.0,
) -> tuple[float, float]:
    """Compute global quantile thresholds on ``index_value``."""
    values = index_df["index_value"].to_numpy(dtype=float)
    q_low, q_high = np.quantile(values, [q_low_pct / 100, q_high_pct / 100], method="linear")
    return float(q_low), float(q_high)


def run_monthly_anomaly_transition(
    result: DecompositionResult,
    *,
    hylak_id: int | None = None,
) -> QuantileResult:
    """Run the one-lake quantile-based anomaly labelling workflow."""
    labels_df = result.index_df

    q_low, q_high = compute_anomaly_thresholds(labels_df)
    labeled_df = assign_extreme_labels(labels_df, q_low, q_high)
    labeled_df["threshold_low"] = q_low
    labeled_df["threshold_high"] = q_high

    if hylak_id is not None:
        labeled_df.insert(0, "hylak_id", hylak_id)
    else:
        labeled_df.insert(0, "hylak_id", pd.Series([pd.NA] * len(labeled_df), dtype="Int64"))

    extremes_df = extract_extreme_events(labeled_df)
    transitions_df = detect_abrupt_transitions(labeled_df)

    extreme = ExtremeResult(
        hylak_id=hylak_id,
        labels_df=labeled_df,
        extremes_df=extremes_df,
        transitions_df=transitions_df,
    )
    diagnostics = QuantileDiagnostics(q_low=q_low, q_high=q_high)

    return QuantileResult(extreme=extreme, diagnostics=diagnostics)
