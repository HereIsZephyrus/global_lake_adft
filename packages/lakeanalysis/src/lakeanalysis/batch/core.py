"""Concrete batch data types.

Separated from ``domain.py`` so that the domain module stays pure
contracts (ABCs / interfaces).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class LakeTask:
    """Per-lake work unit constructed from a ``LakeDataset`` row.

    This is an internal type consumed by ``Calculator.compute``.
    Public callers should use ``Calculator.run_dataset(dataset)`` instead.
    """

    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: frozenset[int]
    extra: dict[str, Any] | None = None
