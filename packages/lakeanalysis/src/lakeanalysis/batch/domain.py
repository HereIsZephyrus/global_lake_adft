"""Domain contracts and data classes for the batch computation framework."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd


@dataclass(frozen=True)
class LakeTask:
    """Per-lake work unit constructed from a LakeDataset row.

    This is an internal type consumed by ``Calculator._compute_lake``.
    Public callers should use ``Calculator.run_dataset(dataset)`` instead.
    """

    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: frozenset[int]
    extra: dict[str, Any] | None = None


class LakeFilter(ABC):
    @abstractmethod
    def __call__(self, hylak_ids: Iterable[int]) -> set[int]: ...


class Calculator(ABC):
    @abstractmethod
    def run_dataset(
        self,
        dataset,
        *,
        error_chunk: tuple[int, int] = (0, 0),
    ) -> tuple[dict[str, list[dict]], int, int]:
        """Execute computation for every lake in *dataset* and return rows."""

    @abstractmethod
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        """Convert a single-lake result into persistence-ready rows."""

    @abstractmethod
    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        """Build error rows for a single lake failure."""
