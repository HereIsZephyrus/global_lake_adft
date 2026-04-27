"""Detect linear interpolation in lake area time series.

A lake is flagged when its water_area series contains at least one
collinear segment of 3+ consecutive **temporally adjacent** points
(2+ identical consecutive diffs), after excluding frozen months and
zero-area observations.

Segments are classified as:
  - "flat": all diffs ≈ 0 (constant value)
  - "linear": non-zero constant diffs (true linear interpolation)

Only points that are adjacent in time (consecutive months) are
considered for collinearity. Gaps in the time series break
segment detection.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class InterpolationConfig:
    rtol: float = 1e-9
    atol: float = 1e-6
    min_collinear_points: int = 4


@dataclass(frozen=True)
class InterpolationResult:
    has_interpolation: bool
    n_linear_segments: int
    n_flat_segments: int
    max_linear_len: int
    max_flat_len: int
    collinear_ratio: float
    first_linear_ym: int | None
    n_obs: int


@dataclass(frozen=True)
class CollinearSegment:
    start_idx: int
    end_idx: int
    length: int
    is_flat: bool
    diff_value: float
    start_ym: int


def _year_month_key(year: int, month: int) -> int:
    return year * 100 + month


def _month_index(year: int, month: int) -> int:
    return year * 12 + (month - 1)


def _prepare_series(
    df: pd.DataFrame,
    frozen_year_months: set[int] | None = None,
) -> pd.DataFrame:
    df = df.copy()
    if frozen_year_months:
        ym = df["year"].astype(int) * 100 + df["month"].astype(int)
        df = df.loc[~ym.isin(frozen_year_months)]
    df = df.loc[df["water_area"] > 0].reset_index(drop=True)
    df = df.sort_values(["year", "month"]).reset_index(drop=True)
    return df


def _adjacent_mask(df: pd.DataFrame) -> np.ndarray:
    years = df["year"].astype(int).to_numpy()
    months = df["month"].astype(int).to_numpy()
    idx_curr = _month_index(years[:-1], months[:-1])
    idx_next = _month_index(years[1:], months[1:])
    return idx_next - idx_curr == 1


def detect_interpolation(
    df: pd.DataFrame,
    frozen_year_months: set[int] | None = None,
    config: InterpolationConfig | None = None,
) -> InterpolationResult:
    if config is None:
        config = InterpolationConfig()

    df = _prepare_series(df, frozen_year_months)

    n_obs = len(df)
    if n_obs < config.min_collinear_points:
        return InterpolationResult(
            has_interpolation=False,
            n_linear_segments=0,
            n_flat_segments=0,
            max_linear_len=0,
            max_flat_len=0,
            collinear_ratio=0.0,
            first_linear_ym=None,
            n_obs=n_obs,
        )

    values = df["water_area"].to_numpy(dtype=float)
    diffs = np.diff(values)
    adjacent = _adjacent_mask(df)

    same = np.isclose(diffs[:-1], diffs[1:], rtol=config.rtol, atol=config.atol)
    valid = same & adjacent[:-1] & adjacent[1:]

    n_linear = 0
    n_flat = 0
    max_linear_len = 0
    max_flat_len = 0
    first_linear_ym: int | None = None
    collinear_indices: set[int] = set()

    i = 0
    while i < len(valid):
        if not valid[i]:
            i += 1
            continue

        run_start = i
        while i < len(valid) and valid[i]:
            i += 1
        run_len = i - run_start + 2

        if run_len >= config.min_collinear_points:
            seg_diffs = diffs[run_start : run_start + run_len - 1]
            is_flat = np.allclose(seg_diffs, 0, atol=config.atol)
            diff_value = float(seg_diffs[0]) if len(seg_diffs) > 0 else 0.0

            for idx in range(run_start, run_start + run_len):
                collinear_indices.add(idx)

            start_ym = _year_month_key(
                int(df.iloc[run_start]["year"]),
                int(df.iloc[run_start]["month"]),
            )

            if is_flat:
                n_flat += 1
                if run_len > max_flat_len:
                    max_flat_len = run_len
            else:
                n_linear += 1
                if run_len > max_linear_len:
                    max_linear_len = run_len
                if first_linear_ym is None:
                    first_linear_ym = start_ym

        i += 1

    ratio = len(collinear_indices) / n_obs if n_obs > 0 else 0.0

    return InterpolationResult(
        has_interpolation=(n_linear + n_flat) > 0,
        n_linear_segments=n_linear,
        n_flat_segments=n_flat,
        max_linear_len=max_linear_len,
        max_flat_len=max_flat_len,
        collinear_ratio=ratio,
        first_linear_ym=first_linear_ym,
        n_obs=n_obs,
    )


def get_collinear_segments(
    df: pd.DataFrame,
    frozen_year_months: set[int] | None = None,
    config: InterpolationConfig | None = None,
) -> list[CollinearSegment]:
    if config is None:
        config = InterpolationConfig()

    df = _prepare_series(df, frozen_year_months)

    if len(df) < config.min_collinear_points:
        return []

    values = df["water_area"].to_numpy(dtype=float)
    diffs = np.diff(values)
    adjacent = _adjacent_mask(df)

    same = np.isclose(diffs[:-1], diffs[1:], rtol=config.rtol, atol=config.atol)
    valid = same & adjacent[:-1] & adjacent[1:]

    segments: list[CollinearSegment] = []

    i = 0
    while i < len(valid):
        if not valid[i]:
            i += 1
            continue

        run_start = i
        while i < len(valid) and valid[i]:
            i += 1
        run_len = i - run_start + 2

        if run_len >= config.min_collinear_points:
            seg_diffs = diffs[run_start : run_start + run_len - 1]
            is_flat = np.allclose(seg_diffs, 0, atol=config.atol)
            diff_value = float(seg_diffs[0]) if len(seg_diffs) > 0 else 0.0

            start_ym = _year_month_key(
                int(df.iloc[run_start]["year"]),
                int(df.iloc[run_start]["month"]),
            )

            segments.append(
                CollinearSegment(
                    start_idx=run_start,
                    end_idx=run_start + run_len - 1,
                    length=run_len,
                    is_flat=is_flat,
                    diff_value=diff_value,
                    start_ym=start_ym,
                )
            )

        i += 1

    return segments
