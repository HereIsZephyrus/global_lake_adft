"""PWMCalculator: batch wrapper for PWM extreme quantile."""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class MultiPWMExtremeResult:
    hylak_id: int | None
    results: list[Any]


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
        threshold_quantiles: list[float] | None = None,
        min_valid_per_month: int | None = None,
        min_valid_observations: int | None = None,
        method: str = "stl",
    ) -> None:
        self._pwm_config = pwm_config or PWMExtremeConfig()
        self._threshold_quantiles = threshold_quantiles or [0.95, 0.99]
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

    def compute(self, task) -> MultiPWMExtremeResult:
        results: list[Any] = []
        for threshold_quantile in self._threshold_quantiles:
            tail_prob = 1.0 - float(threshold_quantile)
            config = PWMExtremeServiceConfig(
                pwm_config=PWMExtremeConfig(
                    n_pwm=self._pwm_config.n_pwm,
                    p_low=tail_prob,
                    p_high=tail_prob,
                    integration_upper=self._pwm_config.integration_upper,
                    l2_regularization=self._pwm_config.l2_regularization,
                    min_observations_per_month=self._pwm_config.min_observations_per_month,
                ),
                min_valid_per_month=self._service_config.min_valid_per_month,
                min_valid_observations=self._service_config.min_valid_observations,
                method=self._service_config.method,
            )
            result = self._service_runner(
                task.series_df,
                hylak_id=task.hylak_id,
                config=config,
                frozen_year_months=set(task.frozen_year_months) or None,
                use_frozen_mask=bool(task.frozen_year_months),
            )
            results.append(result)
        return MultiPWMExtremeResult(hylak_id=task.hylak_id, results=results)

    def result_to_rows(self, result: Any) -> dict[str, list[dict]]:
        if isinstance(result, MultiPWMExtremeResult):
            rows = {
                "pwm_extreme_thresholds": [],
                "pwm_extreme_labels": [],
                "pwm_extreme_extremes": [],
                "pwm_extreme_abrupt_transitions": [],
            }
            for single in result.results:
                rows["pwm_extreme_thresholds"].extend(result_to_threshold_rows(single))
                rows["pwm_extreme_labels"].extend(result_to_label_rows(single))
                rows["pwm_extreme_extremes"].extend(result_to_extreme_rows(single))
                rows["pwm_extreme_abrupt_transitions"].extend(result_to_transition_rows(single))
            return self.with_done_status(rows, result)

        return self.with_done_status({
            "pwm_extreme_thresholds": result_to_threshold_rows(result),
            "pwm_extreme_labels": result_to_label_rows(result),
            "pwm_extreme_extremes": result_to_extreme_rows(result),
            "pwm_extreme_abrupt_transitions": result_to_transition_rows(result),
        }, result)
