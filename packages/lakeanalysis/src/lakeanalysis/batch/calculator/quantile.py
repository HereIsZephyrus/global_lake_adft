"""QuantileCalculator: batch wrapper for quantile anomaly transition."""

from __future__ import annotations

from typing import Any

from lakesource.quantile.schema import (
    CURRENT_QUANTILE_WORKFLOW_VERSION,
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.quantile.store import (
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
)
from lakeanalysis.quantile.service import run_single_lake_service
from lakesource.quantile.schema import QuantileServiceConfig

from ..engine import Calculator, LakeTask


class QuantileCalculator(Calculator):
    def __init__(
        self,
        *,
        min_valid_per_month: int | None = None,
        min_valid_observations: int | None = None,
        workflow_version: str = CURRENT_QUANTILE_WORKFLOW_VERSION,
    ) -> None:
        self._service_config = QuantileServiceConfig(
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
        )
        self._workflow_version = workflow_version

    def run(self, task: LakeTask) -> Any:
        return run_single_lake_service(
            task.series_df,
            hylak_id=task.hylak_id,
            config=self._service_config,
            frozen_year_months=set(task.frozen_year_months) or None,
            use_frozen_mask=bool(task.frozen_year_months),
        )

    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        return {
            "quantile_labels": result_to_label_rows(result, workflow_version=self._workflow_version),
            "quantile_extremes": result_to_extreme_rows(result, workflow_version=self._workflow_version),
            "quantile_abrupt_transitions": result_to_transition_rows(result, workflow_version=self._workflow_version),
            "quantile_run_status": [
                make_run_status_row(
                    hylak_id=result.hylak_id or 0,
                    chunk_start=0,
                    chunk_end=0,
                    workflow_version=self._workflow_version,
                    status=RUN_STATUS_DONE,
                )
            ],
        }

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        return {
            "quantile_run_status": [
                make_run_status_row(
                    hylak_id=hylak_id,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    workflow_version=self._workflow_version,
                    status=RUN_STATUS_ERROR,
                    error_message=str(error),
                )
            ],
        }