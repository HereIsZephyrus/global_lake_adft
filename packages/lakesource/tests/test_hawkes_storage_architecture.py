"""Hawkes architecture storage tests — verify index_value, time, value persistence.

Ensures the storage layer preserves columns needed by downstream Hawkes so
that true two-stage (read from DB, not recompute) is possible in the future.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakesource.pwm_extreme.schema import (
    PWMExtremeMonthResult,
    PWMExtremeResult,
)
from lakesource.pwm_extreme.store import (
    result_to_extreme_rows,
    result_to_label_rows,
)


def _make_labels_df(indexed: bool = True) -> pd.DataFrame:
    row = {
        "hylak_id": 1,
        "year": 2020,
        "month": 6,
        "water_area": 150.0,
        "threshold_low": 30.0,
        "threshold_high": 90.0,
        "extreme_label": "extreme_high",
    }
    if indexed:
        row["index_value"] = 95.0
    return pd.DataFrame([row])


def _make_extremes_df(indexed: bool = True) -> pd.DataFrame:
    row = {
        "hylak_id": 1,
        "year": 2020,
        "month": 6,
        "event_type": "high",
        "water_area": 150.0,
        "threshold": 90.0,
        "severity": 5.0,
        "extreme_label": "extreme_high",
    }
    if indexed:
        row["index_value"] = 95.0
    return pd.DataFrame([row])


class TestStorageIncludesIndexValue:
    """result_to_*_rows MUST include index_value when the column is present."""

    def test_label_rows_include_index_value(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=_make_labels_df(indexed=True),
            extremes_df=pd.DataFrame(),
            transitions_df=pd.DataFrame(),
        )
        rows = result_to_label_rows(result)
        assert len(rows) == 1
        assert "index_value" in rows[0]
        assert rows[0]["index_value"] == 95.0
        assert rows[0]["water_area"] == 150.0

    def test_extreme_rows_include_index_value(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=pd.DataFrame(),
            extremes_df=_make_extremes_df(indexed=True),
            transitions_df=pd.DataFrame(),
        )
        rows = result_to_extreme_rows(result)
        assert len(rows) == 1
        assert "index_value" in rows[0]
        assert rows[0]["index_value"] == 95.0
        assert rows[0]["severity"] == 5.0

    def test_extreme_rows_still_work_without_index_value(self) -> None:
        """Backward compat: index_value column missing → should raise KeyError,
        same as any missing column."""
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=pd.DataFrame(),
            extremes_df=_make_extremes_df(indexed=False),
            transitions_df=pd.DataFrame(),
        )
        with np.testing.assert_raises(KeyError):
            result_to_extreme_rows(result)

    def test_label_rows_still_work_without_index_value(self) -> None:
        result = PWMExtremeResult(
            hylak_id=1,
            month_results=[],
            labels_df=_make_labels_df(indexed=False),
            extremes_df=pd.DataFrame(),
            transitions_df=pd.DataFrame(),
        )
        with np.testing.assert_raises(KeyError):
            result_to_label_rows(result)


class TestEOTExtremeRowsIncludeTimeAndValue:
    """EOT calculator's result_to_rows MUST include time and value columns."""

    def test_extreme_rows_have_time_and_value(self) -> None:
        from lakeanalysis.batch.calculator.eot import EOTCalculator

        calculator = EOTCalculator(tails=["high"], quantiles=[0.90])

        hylak_id = 42
        class _FakeFit:
            converged = True
            log_likelihood = -10.0
            threshold = 100.0
            series = type("s", (), {"n_obs": 240, "data": pd.DataFrame({"year": [2020], "month": [6], "time": [2020.5]})})()
            frozen_year_months = frozenset()
            params = {"beta0": 1.0, "beta1": 0.1, "sin_1": 0.2, "cos_1": 0.3, "sigma": 2.0, "xi": 0.1}
        _FakeFit.extremes = pd.DataFrame([
            {
                "cluster_id": 1, "cluster_size": 1,
                "year": 2020, "month": 6, "time": 2020.5,
                "value": 120.0,
                "original_value": 120.0,
                "threshold": 100.0,
            }
        ])

        from lakeanalysis.batch.calculator.eot import EOTResult
        result = EOTResult(hylak_id=hylak_id, fits=[("high", 0.90, _FakeFit)])

        rows_by_table = calculator.result_to_rows(result)
        extreme_rows = rows_by_table["eot_extremes"]
        assert len(extreme_rows) == 1
        row = extreme_rows[0]
        assert row["hylak_id"] == 42
        assert row["tail"] == "high"
        assert row["time"] == 2020.5
        assert row["value"] == 120.0
        assert row["water_area"] == 120.0
        assert row["threshold_at_event"] == 100.0
