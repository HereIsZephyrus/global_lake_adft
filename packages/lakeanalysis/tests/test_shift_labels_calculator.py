"""Tests for lakeanalysis.quality.shift_labels_calculator."""

from __future__ import annotations

import pandas as pd

from lakeanalysis.batch.engine import LakeTask
from lakeanalysis.quality import ShiftConfig
from lakeanalysis.quality.filters import LakeContext
from lakeanalysis.quality.shift_labels_calculator import ShiftLabelsCalculator


def _make_task(hylak_id: int, values: list[float]) -> LakeTask:
    n = len(values)
    df = pd.DataFrame(
        {
            "year": [2000 + (i // 12) for i in range(n)],
            "month": [(i % 12) + 1 for i in range(n)],
            "water_area": values,
        }
    )
    return LakeTask(
        hylak_id=hylak_id,
        series_df=df,
        frozen_year_months=frozenset(),
    )


def _make_ctx(values: list[float]) -> LakeContext:
    n = len(values)
    df = pd.DataFrame(
        {
            "year": [2000 + (i // 12) for i in range(n)],
            "month": [(i % 12) + 1 for i in range(n)],
            "water_area": values,
        }
    )
    return LakeContext(
        df=df,
        df_no_frozen=df.copy(),
        rs_area_median=float(pd.Series(values).median()),
        rs_area_mean=float(pd.Series(values).mean()),
        rs_area_quantile=float(pd.Series(values).quantile(0.8)),
        atlas_area=0.0,
    )


class TestShiftLabelsCalculator:
    def test_run_degraded_lake(self) -> None:
        calc = ShiftLabelsCalculator(ShiftConfig(min_segment_months=3, smooth_window=3))
        values = [100.0] * 6 + [10.0] * 6
        task = _make_task(123, values)
        result = calc.run(task)

        assert result["hylak_id"] == 123
        assert result["detail"]["label"] == "degraded"
        assert result["detail"]["udmax_break_index"] is not None

    def test_run_stable_lake(self) -> None:
        calc = ShiftLabelsCalculator()
        values = [10.0] * 24
        task = _make_task(456, values)
        result = calc.run(task)

        assert result["hylak_id"] == 456
        assert result["detail"]["label"] == "stable"

    def test_run_intermittent_lake(self) -> None:
        calc = ShiftLabelsCalculator(ShiftConfig(min_segment_months=3, smooth_window=3))
        values = [0.0] * 3 + [50.0] * 3 + [0.0] * 3 + [50.0] * 3
        task = _make_task(789, values)
        result = calc.run(task)

        assert result["hylak_id"] == 789
        assert result["detail"]["label"] == "intermittent"

    def test_result_to_rows_degraded(self) -> None:
        calc = ShiftLabelsCalculator()
        input_result = {
            "hylak_id": 123,
            "detail": {
                "label": "degraded",
                "udmax": 12.5,
                "udmax_p_value": 0.01,
                "udmax_break_index": 6,
                "wdmax": 5.0,
                "wdmax_p_value": 0.05,
                "wdmax_break_index": None,
                "used_deseasoned": False,
                "seasonality_dominance_ratio": 0.1,
            },
        }
        rows = calc.result_to_rows(input_result)

        assert "area_shift_labels" in rows
        assert len(rows["area_shift_labels"]) == 1
        row = rows["area_shift_labels"][0]
        assert row["hylak_id"] == 123
        assert row["shift_label"] == "degraded"
        assert row["udmax"] == 12.5
        assert row["udmax_p_value"] == 0.01
        assert row["udmax_break_index"] == 6

    def test_result_to_rows_stable(self) -> None:
        calc = ShiftLabelsCalculator()
        input_result = {
            "hylak_id": 456,
            "detail": {
                "label": "stable",
                "udmax": 0.0,
                "udmax_p_value": 1.0,
                "udmax_break_index": None,
                "wdmax": 0.0,
                "wdmax_p_value": 1.0,
                "wdmax_break_index": None,
                "used_deseasoned": False,
                "seasonality_dominance_ratio": 0.0,
            },
        }
        rows = calc.result_to_rows(input_result)

        assert "area_shift_labels" in rows
        assert len(rows["area_shift_labels"]) == 1
        row = rows["area_shift_labels"][0]
        assert row["hylak_id"] == 456
        assert row["shift_label"] == "stable"

    def test_error_to_rows(self) -> None:
        calc = ShiftLabelsCalculator()
        rows = calc.error_to_rows(999, ValueError("test error"), 0, 10000)

        assert "area_shift_labels" in rows
        assert len(rows["area_shift_labels"]) == 1
        row = rows["area_shift_labels"][0]
        assert row["hylak_id"] == 999
        assert row["shift_label"] == "stable"
