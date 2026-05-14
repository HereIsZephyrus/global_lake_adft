"""Domain contracts and data classes for the batch computation framework."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd


@dataclass(frozen=True)
class LakeTask:
    hylak_id: int
    series_df: pd.DataFrame
    frozen_year_months: frozenset[int]
    extra: dict[str, Any] | None = None


class LakeFilter(ABC):
    @abstractmethod
    def __call__(self, hylak_ids: Iterable[int]) -> set[int]: ...


class Calculator(ABC):
    @abstractmethod
    def run(self, task: LakeTask) -> Any: ...

    @abstractmethod
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]: ...

    @abstractmethod
    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]: ...

    def run_dataset(
        self,
        dataset,
        *,
        error_chunk: tuple[int, int] = (0, 0),
    ) -> tuple[dict[str, list[dict]], int, int]:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support LakeDataset input"
        )
