"""ComparisonCalculator: run both Quantile and PWM Extreme for one lake."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from lakesource.comparison.schema import (
    CURRENT_COMPARISON_WORKFLOW_VERSION,
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.comparison.store import make_run_status_row
from lakesource.pwm_extreme.schema import (
    CURRENT_PWM_EXTREME_WORKFLOW_VERSION,
    PWMExtremeResult,
    RUN_STATUS_DONE as PWM_DONE,
    RUN_STATUS_ERROR as PWM_ERROR,
)
from lakesource.pwm_extreme.store import (
    make_run_status_row as make_pwm_run_status_row,
    result_to_threshold_rows,
)
from lakesource.quantile.schema import (
    CURRENT_QUANTILE_WORKFLOW_VERSION,
    QuantileResult,
    RUN_STATUS_DONE as Q_DONE,
    RUN_STATUS_ERROR as Q_ERROR,
)
from lakesource.quantile.store import (
    make_run_status_row as make_quantile_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
)
from lakeanalysis.pwm_extreme.service import run_single_lake_service as run_pwm_service
from lakeanalysis.quantile.service import run_single_lake_service as run_quantile_service

from ..batch.engine import Calculator, LakeTask

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ComparisonResult:
    hylak_id: int
    quantile_result: QuantileResult | Exception | None
    pwm_result: PWMExtremeResult | Exception | None


class ComparisonCalculator(Calculator):
    def __init__(
        self,
        *,
        min_valid_per_month: int | None = None,
        min_valid_observations: int | None = None,
        workflow_version: str = CURRENT_COMPARISON_WORKFLOW_VERSION,
    ) -> None:
        self._min_valid_per_month = min_valid_per_month
        self._min_valid_observations = min_valid_observations
        self._workflow_version = workflow_version

    def run(self, task: LakeTask) -> ComparisonResult:
        frozen = set(task.frozen_year_months) or None

        q_result: QuantileResult | Exception | None = None
        try:
            q_result = run_quantile_service(
                task.series_df,
                hylak_id=task.hylak_id,
                frozen_year_months=frozen,
                use_frozen_mask=bool(frozen),
            )
        except Exception as exc:
            log.debug("hylak_id=%d quantile error: %s", task.hylak_id, exc)
            q_result = exc

        pwm_result: PWMExtremeResult | Exception | None = None
        try:
            pwm_result = run_pwm_service(
                task.series_df,
                hylak_id=task.hylak_id,
                frozen_year_months=frozen,
                use_frozen_mask=bool(frozen),
            )
        except Exception as exc:
            log.debug("hylak_id=%d pwm_extreme error: %s", task.hylak_id, exc)
            pwm_result = exc

        return ComparisonResult(
            hylak_id=task.hylak_id,
            quantile_result=q_result,
            pwm_result=pwm_result,
        )

    def result_to_rows(self, result: ComparisonResult) -> dict[str, list[dict]]:
        rows: dict[str, list[dict]] = {}
        q_status = None
        pwm_status = None

        if isinstance(result.quantile_result, QuantileResult):
            rows["quantile_labels"] = result_to_label_rows(
                result.quantile_result,
                workflow_version=CURRENT_QUANTILE_WORKFLOW_VERSION,
            )
            rows["quantile_extremes"] = result_to_extreme_rows(
                result.quantile_result,
                workflow_version=CURRENT_QUANTILE_WORKFLOW_VERSION,
            )
            rows["quantile_abrupt_transitions"] = result_to_transition_rows(
                result.quantile_result,
                workflow_version=CURRENT_QUANTILE_WORKFLOW_VERSION,
            )
            rows["quantile_run_status"] = [
                make_quantile_run_status_row(
                    hylak_id=result.hylak_id,
                    chunk_start=0,
                    chunk_end=0,
                    workflow_version=CURRENT_QUANTILE_WORKFLOW_VERSION,
                    status=Q_DONE,
                )
            ]
            q_status = Q_DONE
        elif isinstance(result.quantile_result, Exception):
            rows["quantile_run_status"] = [
                make_quantile_run_status_row(
                    hylak_id=result.hylak_id,
                    chunk_start=0,
                    chunk_end=0,
                    workflow_version=CURRENT_QUANTILE_WORKFLOW_VERSION,
                    status=Q_ERROR,
                    error_message=str(result.quantile_result),
                )
            ]
            q_status = Q_ERROR

        if isinstance(result.pwm_result, PWMExtremeResult):
            rows["pwm_extreme_thresholds"] = result_to_threshold_rows(
                result.pwm_result,
                workflow_version=CURRENT_PWM_EXTREME_WORKFLOW_VERSION,
            )
            rows["pwm_extreme_run_status"] = [
                make_pwm_run_status_row(
                    hylak_id=result.hylak_id,
                    chunk_start=0,
                    chunk_end=0,
                    workflow_version=CURRENT_PWM_EXTREME_WORKFLOW_VERSION,
                    status=PWM_DONE,
                )
            ]
            pwm_status = PWM_DONE
        elif isinstance(result.pwm_result, Exception):
            rows["pwm_extreme_run_status"] = [
                make_pwm_run_status_row(
                    hylak_id=result.hylak_id,
                    chunk_start=0,
                    chunk_end=0,
                    workflow_version=CURRENT_PWM_EXTREME_WORKFLOW_VERSION,
                    status=PWM_ERROR,
                    error_message=str(result.pwm_result),
                )
            ]
            pwm_status = PWM_ERROR

        overall_status = RUN_STATUS_DONE
        error_parts: list[str] = []
        if q_status == Q_ERROR:
            overall_status = RUN_STATUS_ERROR
            error_parts.append(f"quantile: {result.quantile_result}")
        if pwm_status == PWM_ERROR:
            overall_status = RUN_STATUS_ERROR
            error_parts.append(f"pwm: {result.pwm_result}")

        rows["comparison_run_status"] = [
            make_run_status_row(
                hylak_id=result.hylak_id,
                chunk_start=0,
                chunk_end=0,
                workflow_version=self._workflow_version,
                status=overall_status,
                quantile_status=q_status,
                pwm_status=pwm_status,
                error_message="; ".join(error_parts) if error_parts else None,
            )
        ]
        return rows

    def error_to_rows(
        self, hylak_id: int, error: Exception, chunk_start: int, chunk_end: int
    ) -> dict[str, list[dict]]:
        return {
            "comparison_run_status": [
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