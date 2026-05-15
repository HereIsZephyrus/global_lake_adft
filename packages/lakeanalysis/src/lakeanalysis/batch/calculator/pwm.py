"""PWMCalculator: batch wrapper for PWM extreme quantile."""

from __future__ import annotations

from typing import Any


from lakesource.pwm.schema import (
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.pwm.store import (
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_threshold_rows,
    result_to_transition_rows,
)
from lakeanalysis.pwm.service import run_single_lake_service
from lakesource.pwm.schema import PWMExtremeServiceConfig, PWMExtremeConfig

from .extreme_base import ExtremeBatchCalculator


class PWMCalculator(ExtremeBatchCalculator):
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
        super().__init__(
            PWMExtremeServiceConfig(
                pwm_config=pwm_config or PWMExtremeConfig(),
                min_valid_per_month=min_valid_per_month,
                min_valid_observations=min_valid_observations,
                method=method,
            )
        )

    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        return self.with_done_status({
            "pwm_extreme_thresholds": result_to_threshold_rows(result),
            "pwm_extreme_labels": result_to_label_rows(result),
            "pwm_extreme_extremes": result_to_extreme_rows(result),
            "pwm_extreme_abrupt_transitions": result_to_transition_rows(result),
        }, result)
