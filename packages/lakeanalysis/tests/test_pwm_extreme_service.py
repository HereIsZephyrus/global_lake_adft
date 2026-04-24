"""Tests for lakeanalysis.pwm_extreme.service and store."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.pwm_extreme.service import run_single_lake_service
from lakesource.pwm_extreme.schema import (
    PWMExtremeConfig,
    PWMExtremeServiceConfig,
)
from lakesource.pwm_extreme.store import make_run_status_row, result_to_threshold_rows


@pytest.fixture()
def series_df():
    np.random.seed(42)
    records = []
    for year in range(2000, 2023):
        for month in range(1, 13):
            base = 100.0 + 20.0 * np.sin(2 * np.pi * month / 12)
            area = base * np.random.lognormal(0, 0.2)
            records.append({"year": year, "month": month, "water_area": area})
    return pd.DataFrame(records)


class TestRunSingleLakeService:
    def test_default_config(self, series_df):
        result = run_single_lake_service(series_df, hylak_id=1)
        assert result.hylak_id == 1
        assert len(result.month_results) == 12

    def test_custom_config(self, series_df):
        config = PWMExtremeServiceConfig(
            pwm_config=PWMExtremeConfig(n_pwm=3, p_low=0.10, p_high=0.10),
        )
        result = run_single_lake_service(series_df, hylak_id=2, config=config)
        assert result.hylak_id == 2

    def test_frozen_mask_disabled(self, series_df):
        frozen = {200001, 200002}
        result = run_single_lake_service(
            series_df,
            hylak_id=3,
            frozen_year_months=frozen,
            use_frozen_mask=False,
        )
        assert result.hylak_id == 3


class TestMakeRunStatusRow:
    def test_done(self):
        row = make_run_status_row(
            hylak_id=1,
            chunk_start=0,
            chunk_end=1000,
            workflow_version="pwm-extreme-v1",
            status="done",
        )
        assert row["status"] == "done"
        assert row["error_message"] is None

    def test_invalid_status(self):
        with pytest.raises(ValueError, match="Invalid run status"):
            make_run_status_row(
                hylak_id=1,
                chunk_start=0,
                chunk_end=1000,
                workflow_version="pwm-extreme-v1",
                status="unknown",
            )


class TestResultToThresholdRows:
    def test_row_count(self, series_df):
        result = run_single_lake_service(series_df, hylak_id=1)
        rows = result_to_threshold_rows(result, workflow_version="pwm-extreme-v1")
        assert len(rows) == 12
        assert all("lambda_0" in row for row in rows)
        assert all("b_0" in row for row in rows)
        assert all(row["workflow_version"] == "pwm-extreme-v1" for row in rows)
