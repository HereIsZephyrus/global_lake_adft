"""Unified frozen-month handling for all analysis modules.

Frozen months (when a lake surface is ice-covered) must be excluded from
statistical modelling.  This module centralises:

- Simple row-level filtering (used by quantile / pwm_extreme pipelines).
- Anchor-aware defrozen with plateau schedule (used by EOT pipeline).
- YYYYMM key conversion helpers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FrozenPlateauSchedule:
    """Frozen-period schedule for plateau-aligned model evaluation."""

    anchor_times: np.ndarray
    end_times: np.ndarray


def year_month_to_key(year: int, month: int) -> int:
    """Return a YYYYMM integer key."""
    return year * 100 + month


def year_month_key_to_index(year_month_key: int) -> int:
    """Convert a YYYYMM key to a continuous month index."""
    year = year_month_key // 100
    month = year_month_key % 100
    return year * 12 + (month - 1)


def month_index_to_year_month_key(month_index: int) -> int:
    """Convert a continuous month index back to a YYYYMM key."""
    year = month_index // 12
    month = month_index % 12 + 1
    return year_month_to_key(year, month)


def frozen_run_indices(frozen_year_months: set[int] | None) -> list[tuple[int, int]]:
    """Return contiguous frozen-month runs as inclusive month-index intervals."""
    if not frozen_year_months:
        return []
    month_indices = sorted(year_month_key_to_index(key) for key in frozen_year_months)
    runs: list[tuple[int, int]] = []
    run_start = month_indices[0]
    previous = month_indices[0]
    for month_index in month_indices[1:]:
        if month_index == previous + 1:
            previous = month_index
            continue
        runs.append((run_start, previous))
        run_start = month_index
        previous = month_index
    runs.append((run_start, previous))
    return runs


def first_frozen_months(frozen_year_months: set[int] | None) -> set[int]:
    """Return the first month (YYYYMM key) of each contiguous frozen run."""
    return {
        month_index_to_year_month_key(run_start)
        for run_start, _ in frozen_run_indices(frozen_year_months)
    }


def build_frozen_plateau_schedule(
    frozen_year_months: set[int] | None,
    start_year: int,
) -> FrozenPlateauSchedule | None:
    """Build frozen-run time intervals used to hold fitted values constant."""
    runs = frozen_run_indices(frozen_year_months)
    if not runs:
        return None
    anchor_times: list[float] = []
    end_times: list[float] = []
    for run_start, run_end in runs:
        start_key = month_index_to_year_month_key(run_start)
        end_key = month_index_to_year_month_key(run_end + 1)
        start_key_year = start_key // 100
        start_key_month = start_key % 100
        end_key_year = end_key // 100
        end_key_month = end_key % 100
        anchor_times.append(
            float(start_key_year - start_year) + float(start_key_month - 1) / 12.0
        )
        end_times.append(
            float(end_key_year - start_year) + float(end_key_month - 1) / 12.0
        )
    return FrozenPlateauSchedule(
        anchor_times=np.asarray(anchor_times, dtype=float),
        end_times=np.asarray(end_times, dtype=float),
    )


def apply_frozen_plateau(
    times: np.ndarray,
    values: np.ndarray,
    schedule: FrozenPlateauSchedule | None,
    anchor_values: np.ndarray | None,
) -> np.ndarray:
    """Hold fitted values constant across each frozen run."""
    adjusted = np.asarray(values, dtype=float).copy()
    if schedule is None:
        return adjusted
    if anchor_values is None:
        raise ValueError("anchor_values are required when a frozen plateau schedule is provided")
    times = np.asarray(times, dtype=float)
    anchor_values = np.asarray(anchor_values, dtype=float)
    if len(anchor_values) != len(schedule.anchor_times):
        raise ValueError("anchor_values length must match the frozen plateau schedule")
    epsilon = 1e-10
    for anchor_time, end_time, anchor_value in zip(
        schedule.anchor_times,
        schedule.end_times,
        anchor_values,
        strict=True,
    ):
        mask = (times >= anchor_time - epsilon) & (times < end_time - epsilon)
        adjusted[mask] = anchor_value
    return adjusted


def filter_frozen_rows(
    df: pd.DataFrame,
    frozen_year_months: set[int] | None,
) -> pd.DataFrame:
    """Remove frozen-month rows from a DataFrame.

    Requires columns ``year`` and ``month`` (integer).  Returns the
    original DataFrame unchanged when ``frozen_year_months`` is None or
    empty.
    """
    if not frozen_year_months:
        return df
    year_month = df["year"].astype(int) * 100 + df["month"].astype(int)
    return df.loc[~year_month.isin(frozen_year_months)].reset_index(drop=True)


def defrozen_frame(
    df: pd.DataFrame,
    frozen_year_months: set[int] | None,
) -> pd.DataFrame:
    """Remove frozen months but retain the first month of each frozen run.

    This is the anchor-aware variant used by EOT: the first observation of
    each contiguous frozen run is kept so that the time axis remains
    continuous for plateau interpolation.
    """
    if not frozen_year_months:
        return df
    retained_frozen = first_frozen_months(frozen_year_months)
    removed_frozen = set(frozen_year_months).difference(retained_frozen)
    year_month = df["year"].astype(int) * 100 + df["month"].astype(int)
    result = df.loc[~year_month.isin(removed_frozen)].reset_index(drop=True)
    if result.empty:
        raise ValueError("No observations remain after removing frozen months")
    log.debug(
        "defrozen_frame removed %d month(s) while retaining %d frozen anchor month(s)",
        len(removed_frozen),
        len(retained_frozen),
    )
    return result
