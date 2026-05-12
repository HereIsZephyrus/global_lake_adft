"""PWM extreme store serialization tests (P0).

Validates result_to_* conversion functions that shape numpy arrays and
DataFrames into DB row dicts. These were previously untested.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakesource.pwm_extreme.schema import (
    PWMExtremeMonthResult,
    PWMExtremeResult,
)
from lakesource.pwm_extreme.store import (
    _attach_workflow_version,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_threshold_rows,
    result_to_transition_rows,
)

_WF = "pwm-extreme-v1"


def _make_month_result(
    hylak_id: int = 1,
    month: int = 1,
    mean_area: float = 100.0,
    epsilon: float = 0.0,
    lambda_opt: list[float] | None = None,
    pwm_coefficients: list[float] | None = None,
    threshold_high: float = 200.0,
    threshold_low: float = 50.0,
    converged: bool = True,
    objective_value: float = 0.123,
) -> PWMExtremeMonthResult:
    if lambda_opt is None:
        lambda_opt = [1.0, 2.0, 3.0, 4.0, 5.0]
    if pwm_coefficients is None:
        pwm_coefficients = [0.1, 0.2, 0.3, 0.4, 0.5]
    return PWMExtremeMonthResult(
        hylak_id=hylak_id,
        month=month,
        mean_area=mean_area,
        epsilon=epsilon,
        lambda_opt=np.array(lambda_opt, dtype=float),
        pwm_coefficients=np.array(pwm_coefficients, dtype=float),
        threshold_high=threshold_high,
        threshold_low=threshold_low,
        converged=converged,
        objective_value=objective_value,
    )


def _make_labels_df(hylak_id: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "hylak_id": hylak_id,
                "year": 2000,
                "month": 1,
                "water_area": 95.0,
                "threshold_low": 50.0,
                "threshold_high": 200.0,
                "extreme_label": "none",
            },
            {
                "hylak_id": hylak_id,
                "year": 2000,
                "month": 2,
                "water_area": 210.0,
                "threshold_low": 50.0,
                "threshold_high": 200.0,
                "extreme_label": "high",
            },
        ]
    )


def _make_extremes_df(hylak_id: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "hylak_id": hylak_id,
                "year": 2000,
                "month": 2,
                "event_type": "high",
                "water_area": 210.0,
                "threshold": 200.0,
                "severity": 10.0,
                "extreme_label": "high",
            }
        ]
    )


def _make_transitions_df(hylak_id: int = 1) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "hylak_id": hylak_id,
                "from_year": 2000,
                "from_month": 1,
                "to_year": 2000,
                "to_month": 2,
                "transition_type": "none->high",
                "from_water_area": 95.0,
                "to_water_area": 210.0,
                "from_label": "none",
                "to_label": "high",
            }
        ]
    )


class TestResultToThresholdRows:
    def test_single_month(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[_make_month_result()],
            labels_df=_make_labels_df(),
            extremes_df=_make_extremes_df(),
            transitions_df=_make_transitions_df(),
        )
        rows = result_to_threshold_rows(result, workflow_version=_WF)
        assert len(rows) == 1
        row = rows[0]
        assert row["hylak_id"] == 1
        assert row["month"] == 1
        assert row["mean_area"] == 100.0
        assert row["epsilon"] == 0.0
        assert row["converged"] is True
        assert row["objective_value"] == 0.123
        assert row["workflow_version"] == _WF

    def test_lambda_and_pwm_keys(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[_make_month_result()],
            labels_df=_make_labels_df(),
            extremes_df=_make_extremes_df(),
            transitions_df=_make_transitions_df(),
        )
        rows = result_to_threshold_rows(result, workflow_version=_WF)
        row = rows[0]
        assert row["lambda_0"] == 1.0
        assert row["lambda_1"] == 2.0
        assert row["lambda_2"] == 3.0
        assert row["lambda_3"] == 4.0
        assert row["lambda_4"] == 5.0
        assert row["b_0"] == 0.1
        assert row["b_4"] == 0.5

    def test_empty_month_results(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=_make_labels_df(),
            extremes_df=_make_extremes_df(),
            transitions_df=_make_transitions_df(),
        )
        rows = result_to_threshold_rows(result, workflow_version=_WF)
        assert rows == []

    def test_different_lambda_length(self) -> None:
        mr = _make_month_result(lambda_opt=[1.0, 2.0, 3.0])  # length 3, not 5
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[mr],
            labels_df=_make_labels_df(),
            extremes_df=_make_extremes_df(),
            transitions_df=_make_transitions_df(),
        )
        rows = result_to_threshold_rows(result, workflow_version=_WF)
        row = rows[0]
        assert row["lambda_0"] == 1.0
        assert row["lambda_1"] == 2.0
        assert row["lambda_2"] == 3.0
        assert "lambda_3" not in row
        assert "lambda_4" not in row

    def test_multiple_months(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[
                _make_month_result(month=1),
                _make_month_result(month=2),
                _make_month_result(month=3),
            ],
            labels_df=_make_labels_df(),
            extremes_df=_make_extremes_df(),
            transitions_df=_make_transitions_df(),
        )
        rows = result_to_threshold_rows(result, workflow_version=_WF)
        assert len(rows) == 3
        assert [r["month"] for r in rows] == [1, 2, 3]


class TestResultToLabelRows:
    def test_basic(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=_make_labels_df(),
            extremes_df=pd.DataFrame(),
            transitions_df=pd.DataFrame(),
        )
        rows = result_to_label_rows(result, workflow_version=_WF)
        assert len(rows) == 2
        assert all("workflow_version" in r for r in rows)
        assert all(r["workflow_version"] == _WF for r in rows)
        assert rows[0]["extreme_label"] == "none"
        assert rows[1]["extreme_label"] == "high"


class TestResultToExtremeRows:
    def test_basic(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=pd.DataFrame(),
            extremes_df=_make_extremes_df(),
            transitions_df=pd.DataFrame(),
        )
        rows = result_to_extreme_rows(result, workflow_version=_WF)
        assert len(rows) == 1
        assert rows[0]["event_type"] == "high"
        assert rows[0]["workflow_version"] == _WF

    def test_empty(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=pd.DataFrame(),
            extremes_df=pd.DataFrame(
                columns=[
                    "hylak_id", "year", "month", "event_type",
                    "water_area", "threshold", "severity", "extreme_label",
                ]
            ),
            transitions_df=pd.DataFrame(),
        )
        rows = result_to_extreme_rows(result, workflow_version=_WF)
        assert rows == []


class TestResultToTransitionRows:
    def test_basic(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=pd.DataFrame(),
            extremes_df=pd.DataFrame(),
            transitions_df=_make_transitions_df(),
        )
        rows = result_to_transition_rows(result, workflow_version=_WF)
        assert len(rows) == 1
        assert rows[0]["transition_type"] == "none->high"
        assert rows[0]["workflow_version"] == _WF

    def test_empty(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=pd.DataFrame(),
            extremes_df=pd.DataFrame(),
            transitions_df=pd.DataFrame(
                columns=[
                    "hylak_id", "from_year", "from_month", "to_year",
                    "to_month", "transition_type", "from_water_area",
                    "to_water_area", "from_label", "to_label",
                ]
            ),
        )
        rows = result_to_transition_rows(result, workflow_version=_WF)
        assert rows == []


class TestAttachWorkflowVersion:
    def test_attaches(self) -> None:
        rows = _attach_workflow_version([{"a": 1}], workflow_version="v2")
        assert rows == [{"a": 1, "workflow_version": "v2"}]

    def test_strips(self) -> None:
        rows = _attach_workflow_version([{"a": 1}], workflow_version="  v2  ")
        assert rows == [{"a": 1, "workflow_version": "v2"}]

    def test_rejects_empty_after_strip(self) -> None:
        with pytest.raises(ValueError, match="workflow_version must not be empty"):
            _attach_workflow_version([{"a": 1}], workflow_version="   ")

    def test_rejects_empty_string(self) -> None:
        with pytest.raises(ValueError, match="workflow_version must not be empty"):
            _attach_workflow_version([{"a": 1}], workflow_version="")


class TestMakeRunStatusRow:
    def test_done(self) -> None:
        row = make_run_status_row(
            hylak_id=1,
            chunk_start=0,
            chunk_end=100,
            workflow_version=_WF,
            status="done",
        )
        assert row["hylak_id"] == 1
        assert row["status"] == "done"
        assert row["error_message"] is None

    def test_error(self) -> None:
        row = make_run_status_row(
            hylak_id=1,
            chunk_start=0,
            chunk_end=100,
            workflow_version=_WF,
            status="error",
            error_message="something broke",
        )
        assert row["status"] == "error"
        assert row["error_message"] == "something broke"

    def test_rejects_invalid_status(self) -> None:
        with pytest.raises(ValueError, match="Invalid run status"):
            make_run_status_row(
                hylak_id=1,
                chunk_start=0,
                chunk_end=100,
                workflow_version=_WF,
                status="unknown",
            )
