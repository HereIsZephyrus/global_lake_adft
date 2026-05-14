"""PWMExtremeCalculator: batch wrapper for PWM extreme quantile."""

from __future__ import annotations

from typing import Any

from collections import defaultdict

from lakesource.pwm_extreme.schema import (
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.pwm_extreme.store import (
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_threshold_rows,
    result_to_transition_rows,
)
from lakeanalysis.pwm_extreme.service import run_single_lake_service
from lakesource.pwm_extreme.schema import PWMExtremeServiceConfig, PWMExtremeConfig

from ..domain import Calculator, LakeTask
from ..lake_dataset import LakeDataset


class PWMExtremeCalculator(Calculator):
    def __init__(
        self,
        *,
        pwm_config: PWMExtremeConfig | None = None,
        min_valid_per_month: int | None = None,
        min_valid_observations: int | None = None,
        method: str = "stl",
    ) -> None:
        self._service_config = PWMExtremeServiceConfig(
            pwm_config=pwm_config or PWMExtremeConfig(),
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
            method=method,
        )

    def _compute_lake(self, task: LakeTask) -> Any:
        return run_single_lake_service(
            task.series_df,
            hylak_id=task.hylak_id,
            config=self._service_config,
            frozen_year_months=set(task.frozen_year_months) or None,
            use_frozen_mask=bool(task.frozen_year_months),
        )

    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        return {
            "pwm_extreme_thresholds": result_to_threshold_rows(result),
            "pwm_extreme_labels": result_to_label_rows(result),
            "pwm_extreme_extremes": result_to_extreme_rows(result),
            "pwm_extreme_abrupt_transitions": result_to_transition_rows(result),
            "pwm_extreme_run_status": [
                make_run_status_row(
                    hylak_id=result.hylak_id or 0,
                    chunk_start=0,
                    chunk_end=0,
                    status=RUN_STATUS_DONE,
                )
            ],
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        return {
            "pwm_extreme_run_status": [
                make_run_status_row(
                    hylak_id=hylak_id,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    status=RUN_STATUS_ERROR,
                    error_message=str(error),
                )
            ],
        }

    def run_dataset(
        self,
        dataset: LakeDataset,
        *,
        error_chunk: tuple[int, int] = (0, 0),
    ) -> tuple[dict[str, list[dict]], int, int]:
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
                    task.hylak_id,
                    exc,
                    chunk_start,
                    chunk_end,
                ).items():
                    all_rows[table].extend(rows)
                error_lakes += 1
        return dict(all_rows), success_lakes, error_lakes
