"""PWMHawkesCalculator: PWM event -> exponential decay S_k -> Hawkes fit.

Uses ``run_single_lake_service`` (STL decomposition + pooled PWM) so that
the event-detection math is identical to the standalone PWM batch pipeline.

Replaces the old hard-threshold runs declustering with exponential
decay strength S_k and transition/unilateral segment extraction.
"""

from __future__ import annotations

import logging
from typing import Literal

import numpy as np
import pandas as pd

from lakeanalysis.hawkes import (
    HawkesCoreResult,
    build_error_summary,
    build_events_from_pwm,
    run_hawkes_pipeline,
)
from lakeanalysis.pwm.events import (
    compute_decay_index,
    extract_hawkes_events_from_segments,
)
from lakeanalysis.pwm.evt import compute_pwm_evt_strengths
from lakeanalysis.pwm.phi import map_strength_df_to_phi
from lakesource.pwm.store import return_levels_to_rows
from lakeanalysis.pwm.service import run_single_lake_service
from lakesource.pwm.schema import (
    PWMExtremeConfig,
    PWMExtremeServiceConfig,
)

from .. import LakeTask
from .hawkes_base import HawkesCalculator, HawkesResult

log = logging.getLogger(__name__)
class PWMHawkesCalculator(HawkesCalculator):
    """Batch calculator: PWM extreme events + decay index → Hawkes fitting."""

    _BRIDGE_SK_THRESHOLD = 1.0

    def __init__(
        self,
        *,
        pwm_config: PWMExtremeConfig | None = None,
        decay_rate: float = 0.8,
        hawkes_window_months: float = 4.0,
        monthly_significance_quantile: float = 0.95,
        method: str = "stl",
        evt_route: Literal["A", "B"] = "A",
        phi_method: str = "identity",
    ) -> None:
        super().__init__(
            hawkes_window_months=hawkes_window_months,
            monthly_significance_quantile=monthly_significance_quantile,
        )
        self._table_prefix = "pwm_hawkes"
        self._return_levels_table = "pwm_extreme_return_levels"
        self._pwm_config = pwm_config or PWMExtremeConfig()
        self._service_config = PWMExtremeServiceConfig(
            pwm_config=self._pwm_config,
            method=method,
        )
        self._decay_rate = decay_rate
        self._evt_route = evt_route
        self._phi_method = phi_method

    def compute(self, task: LakeTask) -> HawkesResult:
        hylak_id = task.hylak_id
        series_df = task.series_df
        frozen = set(task.frozen_year_months) if task.frozen_year_months else set()

        try:
            pwm_result = run_single_lake_service(
                series_df,
                hylak_id=hylak_id,
                config=self._service_config,
                frozen_year_months=frozen or None,
            )
            strengths_df, summary_df = compute_pwm_evt_strengths(
                pwm_result.labels_df,
                evt_route=self._evt_route,
            )
            phi_df = map_strength_df_to_phi(
                strengths_df,
                method=self._phi_method,
            )

            decay_df = compute_decay_index(
                pwm_result.labels_df,
                decay_rate=self._decay_rate,
                phi_df=phi_df,
            )
            segments_df = _extract_segments_bidirectional(
                pwm_result.labels_df,
                decay_df,
                decay_rate=self._decay_rate,
                bridge_sk_threshold=self._BRIDGE_SK_THRESHOLD,
            )
            segments_rows = _build_segments_rows(hylak_id, segments_df)
            return_level_rows = return_levels_to_rows(hylak_id, summary_df)
            route_summary_rows = _build_route_summary_rows(
                hylak_id=hylak_id,
                route=self._evt_route,
                phi_method=self._phi_method,
                strengths_df=strengths_df,
                segments_df=segments_df,
                summary_df=summary_df,
            )

            events_df = extract_hawkes_events_from_segments(
                pwm_result.labels_df, decay_df, segments_df
            )

            event_series, events_table = build_events_from_pwm(
                events_df, series_df
            )

            core = run_hawkes_pipeline(
                event_series,
                events_table,
                series_df,
                hylak_id=hylak_id,
                threshold_quantile=0.0,
                hawkes_window_months=self._hawkes_window_months,
                monthly_significance_quantile=self._monthly_significance_quantile,
            )
            return self._make_result(
                core,
                return_level_rows=return_level_rows,
                extra_rows_by_table={
                    "pwm_hawkes_segments": segments_rows,
                    "pwm_hawkes_route_summary": route_summary_rows,
                },
            )
        except Exception as exc:
            log.debug("PWM-Hawkes failed for hylak_id=%d: %s", task.hylak_id, exc)
            error_summary = build_error_summary(hylak_id, str(exc))
            return self._make_result(
                HawkesCoreResult(
                    summary=error_summary, lrt_rows=[], transition_monthly_rows=[]
                ),
                extra_rows_by_table=self._empty_extra_rows(),
            )

    def _empty_extra_rows(self) -> dict[str, list[dict]]:
        return {
            "pwm_hawkes_segments": [],
            "pwm_hawkes_route_summary": [],
        }


def _build_segments_rows(hylak_id: int, segments_df: pd.DataFrame) -> list[dict]:
    rows: list[dict] = []
    if segments_df.empty:
        return rows
    for _, seg in segments_df.iterrows():
        rows.append(
            {
                "hylak_id": hylak_id,
                "segment_id": int(seg["segment_id"]),
                "start_year": int(seg["start_year"]),
                "start_month": int(seg["start_month"]),
                "end_year": int(seg["end_year"]),
                "end_month": int(seg["end_month"]),
                "duration_months": int(seg["duration_months"]),
                "segment_type": str(seg["segment_type"]),
                "has_high": bool(seg["has_high"]),
                "has_low": bool(seg["has_low"]),
                "max_S": float(seg["max_S"]),
                "mean_S": float(seg["mean_S"]),
                "integral_S": float(seg["integral_S"]),
                "n_extreme_events": int(seg["n_extreme_events"]),
                "first_extreme_type": (
                    str(seg["first_extreme_type"])
                    if seg.get("first_extreme_type") is not None
                    else None
                ),
                "last_extreme_type": (
                    str(seg["last_extreme_type"])
                    if seg.get("last_extreme_type") is not None
                    else None
                ),
            }
        )
    return rows


def _build_route_summary_rows(
    *,
    hylak_id: int,
    route: str,
    phi_method: str,
    strengths_df: pd.DataFrame,
    segments_df: pd.DataFrame,
    summary_df: pd.DataFrame,
) -> list[dict]:
    high_df = strengths_df[strengths_df["tail"] == "high"] if not strengths_df.empty else pd.DataFrame()
    low_df = strengths_df[strengths_df["tail"] == "low"] if not strengths_df.empty else pd.DataFrame()
    transition_df = segments_df[segments_df["segment_type"] == "transition"] if not segments_df.empty else pd.DataFrame()
    return [
        {
            "hylak_id": hylak_id,
            "evt_route": route,
            "phi_method": phi_method,
            "n_extreme_high": int(len(high_df)),
            "n_extreme_low": int(len(low_df)),
            "mean_strength_high": float(high_df["event_strength"].mean()) if not high_df.empty else None,
            "mean_strength_low": float(low_df["event_strength"].mean()) if not low_df.empty else None,
            "n_segments": int(len(segments_df)),
            "n_transition_segments": int(len(transition_df)),
            "mean_segment_duration": float(segments_df["duration_months"].mean()) if not segments_df.empty else None,
            "n_return_level_fits": int(summary_df["converged"].fillna(False).sum()) if not summary_df.empty else 0,
        }
    ]


def _extract_segments_bidirectional(
    labeled_df: pd.DataFrame,
    decay_df: pd.DataFrame,
    *,
    decay_rate: float,
    bridge_sk_threshold: float,
) -> pd.DataFrame:
    """Build segments using bidirectional bridge support around one normal month."""
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
        raise ValueError(f"decay_df missing required columns: {sorted(missing)}")

    df = decay_df.sort_values(["year", "month"]).reset_index(drop=True).copy()
    df["month_ordinal"] = df["year"].astype(int) * 12 + df["month"].astype(int) - 1
    labels = labeled_df.sort_values(["year", "month"]).reset_index(drop=True).copy()
    labels["month_ordinal"] = labels["year"].astype(int) * 12 + labels["month"].astype(int) - 1
    support_by_ordinal = {
        int(row["month_ordinal"]): _extreme_support_value(row)
        for _, row in labels.iterrows()
    }

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
            first_extreme_type = "high" if bool(extreme_rows.iloc[0]["has_high"]) else "low"
            last_extreme_type = "high" if bool(extreme_rows.iloc[-1]["has_high"]) else "low"

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
        if consecutive_normals >= 2:
            close_segment(seg_end if seg_end is not None else idx - 1)
            continue

        if idx == 0 or idx == n - 1:
            close_segment(seg_end if seg_end is not None else idx - 1)
            continue

        prev_row = df.iloc[idx - 1]
        next_row = df.iloc[idx + 1]
        if not (bool(prev_row["has_high"] or prev_row["has_low"]) and bool(next_row["has_high"] or next_row["has_low"])):
            close_segment(seg_end if seg_end is not None else idx - 1)
            continue

        gap_left = int(row["month_ordinal"]) - int(prev_row["month_ordinal"])
        gap_right = int(next_row["month_ordinal"]) - int(row["month_ordinal"])
        support = (
            float(support_by_ordinal.get(int(prev_row["month_ordinal"]), 0.0)) * float(np.exp(-decay_rate * gap_left))
            + float(support_by_ordinal.get(int(next_row["month_ordinal"]), 0.0)) * float(np.exp(-decay_rate * gap_right))
        )
        if support <= bridge_sk_threshold:
            close_segment(seg_end if seg_end is not None else idx - 1)
            continue

        seg_end = idx

    if seg_start is not None and seg_end is not None:
        close_segment(seg_end)

    if not segments:
        return empty_result
    return pd.DataFrame(segments)


def _extreme_support_value(row: pd.Series) -> float:
    label = str(row.get("extreme_label", ""))
    if label == "extreme_high":
        return abs(float(row["index_value"]) - float(row["threshold_high"]))
    if label == "extreme_low":
        return abs(float(row["threshold_low"]) - float(row["index_value"]))
    return 0.0
