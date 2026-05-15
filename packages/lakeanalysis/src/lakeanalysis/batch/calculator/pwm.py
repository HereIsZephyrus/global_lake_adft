"""PWMCalculator: batch wrapper for PWM extreme quantile."""

from __future__ import annotations

from typing import Any


from lakesource.pwm.schema import RUN_STATUS_DONE, RUN_STATUS_ERROR
from lakesource.pwm.store import (
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_threshold_rows,
    result_to_transition_rows,
)
from lakesource.pwm.schema import PWMExtremeServiceConfig, PWMExtremeConfig
from lakeanalysis.pwm.service import run_single_lake_service

from .extreme_base import ExtremeBatchCalculator


class PWMCalculator(ExtremeBatchCalculator):
    """Batch wrapper for PWM extreme quantile results."""

    _run_status_table = "pwm_extreme_run_status"
    _run_status_done = RUN_STATUS_DONE
    _run_status_error = RUN_STATUS_ERROR
    _service_runner = staticmethod(run_single_lake_service)
    _run_status_builder = staticmethod(make_run_status_row)

    def __init__(
        self,
        *,
        pwm_config: PWMExtremeConfig | None = None,
        min_valid_per_month: int | None = None,
        min_valid_observations: int | None = None,
        method: str = "stl",
    ) -> None:
        self._pwm_config = pwm_config or PWMExtremeConfig()
        super().__init__(
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
            method=method,
        )

    def build_service_config(
        self,
        *,
        min_valid_per_month: int | None,
        min_valid_observations: int | None,
        method: str,
    ) -> PWMExtremeServiceConfig:
        return PWMExtremeServiceConfig(
            pwm_config=self._pwm_config,
            min_valid_per_month=min_valid_per_month,
            min_valid_observations=min_valid_observations,
            method=method,
        )

    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        return self.with_done_status({
            "pwm_extreme_thresholds": result_to_threshold_rows(result),
            "pwm_extreme_labels": result_to_label_rows(result),
            "pwm_extreme_extremes": result_to_extreme_rows(result),
            "pwm_extreme_abrupt_transitions": result_to_transition_rows(result),
        }, result)
