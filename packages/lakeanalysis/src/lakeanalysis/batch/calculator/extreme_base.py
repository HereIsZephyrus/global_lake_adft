"""Small shared base for single-lake extreme batch calculators."""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Callable
from typing import Any

from .base import Calculator
from .. import LakeTask


class ExtremeBatchCalculator(Calculator):
    """Share single-lake service execution and run-status row handling."""

    _run_status_table: str
    _run_status_done: str
    _run_status_error: str
    _service_runner: Callable[..., Any]
    _run_status_builder: Callable[..., dict]

    def __init__(
        self,
        *,
        min_valid_per_month: int | None = None,
        min_valid_observations: int | None = None,
        method: str = "stl",
    ) -> None:
        self._service_config = self.build_service_config(
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
            method=method,
        )

    @abstractmethod
    def build_service_config(
        self,
        *,
        min_valid_per_month: int | None,
        min_valid_observations: int | None,
        method: str,
    ) -> Any:
        """Build the service config for the specific extreme algorithm."""

    def compute(self, task: LakeTask) -> Any:
        return self._service_runner(
            task.series_df,
            hylak_id=task.hylak_id,
            config=self._service_config,
            frozen_year_months=set(task.frozen_year_months) or None,
            use_frozen_mask=bool(task.frozen_year_months),
        )

    @abstractmethod
    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        """Convert a single-lake result into persistence-ready rows."""

    def _run_status_rows(
        self,
        *,
        hylak_id: int,
        chunk_start: int,
        chunk_end: int,
        status: str,
        error_message: str | None = None,
    ) -> dict[str, list[dict]]:
        return {
            self._run_status_table: [
                self._run_status_builder(
                    hylak_id=hylak_id,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    status=status,
                    error_message=error_message,
                )
            ]
        }

    def with_done_status(
        self, rows: dict[str, list[dict]], result: Any
    ) -> dict[str, list[dict]]:
        """Attach the standard success run-status row to result rows."""
        rows.update(
            self._run_status_rows(
                hylak_id=result.hylak_id or 0,
                chunk_start=0,
                chunk_end=0,
                status=self._run_status_done,
            )
        )
        return rows

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        return self._run_status_rows(
            hylak_id=hylak_id,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            status=self._run_status_error,
            error_message=str(error),
        )
