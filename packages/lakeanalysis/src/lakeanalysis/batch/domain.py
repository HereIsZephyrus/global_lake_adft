"""Domain contracts for the batch computation framework.

Concrete data types live in ``core.py`` so that this module stays
pure contracts (ABCs / interfaces).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from .core import LakeTask


class LakeFilter(ABC):
    @abstractmethod
    def __call__(self, hylak_ids: Iterable[int]) -> set[int]: ...


class Calculator(ABC):
    @abstractmethod
    def _compute_lake(self, task: LakeTask) -> Any:
        """Per-lake computation logic.

        Subclasses implement this instead of the old ``run(task)`` method.
        The shared ``run_dataset`` default calls this once per lake.
        """

    @abstractmethod
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        """Convert a single-lake result into persistence-ready rows."""

    @abstractmethod
    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        """Build error rows for a single lake failure."""

    def run_dataset(
        self,
        dataset,
        *,
        error_chunk: tuple[int, int] = (0, 0),
    ) -> tuple[dict[str, list[dict]], int, int]:
        """Default implementation: iterate by building a ``LakeTask`` per row."""
        all_rows: dict[str, list[dict]] = defaultdict(list)
        success_lakes = 0
        error_lakes = 0
        chunk_start, chunk_end = error_chunk

        for idx in range(len(dataset)):
            task = dataset.to_task(idx)
            try:
                result = self._compute_lake(task)
                for table, rows in self.result_to_rows(result).items():
                    all_rows[table].extend(rows)
                success_lakes += 1
            except Exception as exc:
                for table, rows in self.error_to_rows(
                    task.hylak_id, exc, chunk_start, chunk_end
                ).items():
                    all_rows[table].extend(rows)
                error_lakes += 1
        return dict(all_rows), success_lakes, error_lakes
