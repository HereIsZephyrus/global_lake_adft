import numpy as np
import pandas as pd
import pytest

from lakeanalysis.batch.task_spec import BatchTaskSpec, get_batch_task_spec
from lakeanalysis.pwm_extreme.diagnostics import (
    pwm_constraint_residuals,
    quantile_function_curve,
)
from lakeanalysis.hawkes.bridge import _events_with_label, build_events_from_eot
from lakeanalysis.hawkes.types import TYPE_DRY, TYPE_LABELS, TYPE_WET


# ─── batch/task_spec.py ─────────────────────────────────────────────────────


class TestBatchTaskSpec:
    def test_defaults(self):
        spec = BatchTaskSpec()
        assert spec.done_table is None
        assert spec.done_requires_status is False
        assert spec.ensure_tables == ()

    def test_quantile_spec(self):
        spec = get_batch_task_spec("quantile")
        assert spec.done_table == "quantile_run_status"
        assert spec.done_requires_status is True
        assert "quantile_labels" in spec.ensure_tables

    def test_pwm_extreme_spec(self):
        spec = get_batch_task_spec("pwm_extreme")
        assert spec.done_table == "pwm_extreme_run_status"

    def test_eot_spec(self):
        spec = get_batch_task_spec("eot")
        assert spec.done_table == "eot_run_status"

    def test_comparison_spec(self):
        spec = get_batch_task_spec("comparison")
        assert "comparison_run_status" in spec.ensure_tables
        assert "quantile_labels" in spec.ensure_tables

    def test_shift_labels_spec(self):
        spec = get_batch_task_spec("shift_labels")
        assert spec.done_table == "area_shift_labels"
        assert spec.done_requires_status is False

    def test_unknown_returns_default(self):
        spec = get_batch_task_spec("nonexistent")
        assert spec.done_table is None
        assert spec.ensure_tables == ()


# ─── pwm_extreme/diagnostics.py ─────────────────────────────────────────────


class TestQuantileFunctionCurve:
    def test_basic_shape(self):
        lambda_opt = np.array([0.1, -0.05])
        epsilon = 0.5
        result = quantile_function_curve(lambda_opt, epsilon, n_points=50)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 50
        assert list(result.columns) == ["u", "prior_y", "fitted_x"]

    def test_u_range(self):
        lambda_opt = np.array([0.1])
        epsilon = 1.0
        result = quantile_function_curve(lambda_opt, epsilon, n_points=100)
        assert result["u"].iloc[0] == pytest.approx(0.0)
        assert result["u"].iloc[-1] < 1.0

    def test_prior_monotone(self):
        lambda_opt = np.array([0.0])
        epsilon = 1.0
        result = quantile_function_curve(lambda_opt, epsilon, n_points=50)
        assert np.all(np.diff(result["prior_y"].values) >= 0)


class TestPwmConstraintResiduals:
    def test_basic(self):
        lambda_opt = np.array([0.1, -0.02])
        b_target = np.array([1.0, 0.6, 0.4])
        epsilon = 0.5
        result = pwm_constraint_residuals(lambda_opt, b_target, epsilon)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert list(result.columns) == ["k", "b_target", "b_fitted", "residual"]
        assert list(result["k"]) == [0, 1, 2]

    def test_residual_is_difference(self):
        lambda_opt = np.array([0.05])
        b_target = np.array([2.0, 1.0])
        epsilon = 1.0
        result = pwm_constraint_residuals(lambda_opt, b_target, epsilon)
        for _, row in result.iterrows():
            assert row["residual"] == pytest.approx(
                row["b_fitted"] - row["b_target"]
            )


# ─── hawkes/bridge.py ───────────────────────────────────────────────────────


class TestEventsWithLabel:
    def test_empty_extremes(self):
        df = pd.DataFrame(columns=[
            "time", "year", "month", "value", "original_value", "threshold"
        ])
        result = _events_with_label(df, TYPE_LABELS[TYPE_WET])
        assert result.empty
        assert "event_label" in result.columns
        assert "event_type" in result.columns

    def test_wet_label(self):
        df = pd.DataFrame({
            "time": [0.5],
            "year": [2000],
            "month": [7],
            "value": [1.5],
            "original_value": [150.0],
            "threshold": [1.2],
        })
        result = _events_with_label(df, TYPE_LABELS[TYPE_WET])
        assert len(result) == 1
        assert result.iloc[0]["event_type"] == TYPE_WET
        assert result.iloc[0]["event_label"] == TYPE_LABELS[TYPE_WET]

    def test_dry_label(self):
        df = pd.DataFrame({
            "time": [0.5],
            "year": [2000],
            "month": [7],
            "value": [-1.5],
            "original_value": [50.0],
            "threshold": [-1.2],
        })
        result = _events_with_label(df, TYPE_LABELS[TYPE_DRY])
        assert result.iloc[0]["event_type"] == TYPE_DRY


class TestBuildEventsFromEot:
    def test_synthetic_series(self):
        rng = np.random.default_rng(42)
        n = 120
        years = [2000 + i // 12 for i in range(n)]
        months = [i % 12 + 1 for i in range(n)]
        areas = 100.0 + 20.0 * np.sin(np.linspace(0, 10 * np.pi, n))
        areas += rng.normal(0, 5, n)
        areas[50] = 200.0
        areas[80] = 20.0
        df = pd.DataFrame({
            "year": years,
            "month": months,
            "water_area": areas,
        })
        result = build_events_from_eot(df, threshold_quantile=0.90)
        assert result.times.size > 0
        assert result.start_time < result.end_time
        assert len(result.events_table) == result.times.size
        assert set(result.event_types).issubset({TYPE_DRY, TYPE_WET})
