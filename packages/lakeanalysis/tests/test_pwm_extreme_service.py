"""Tests for lakeanalysis.pwm.service and store."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.pwm.service import run_single_lake_service
from lakesource.pwm.schema import (
    PWMExtremeConfig,
    PWMExtremeServiceConfig,
)
from lakesource.pwm.store import (
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_threshold_rows,
    result_to_transition_rows,
)


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
                status="unknown",
            )


class TestResultToThresholdRows:
    def test_row_count(self, series_df):
        result = run_single_lake_service(series_df, hylak_id=1)
        rows = result_to_threshold_rows(result)
        assert len(rows) == 12
        assert all("lambda_0" in row for row in rows)
        assert all("b_0" in row for row in rows)


class TestResultToLabelRows:
    def test_has_label_columns(self, series_df):
        result = run_single_lake_service(series_df, hylak_id=1)
        rows = result_to_label_rows(result)
        assert len(rows) > 0
        for row in rows:
            assert "hylak_id" in row
            assert "year" in row
            assert "month" in row
            assert "water_area" in row
            assert "extreme_label" in row


class TestResultToExtremeRows:
    def test_extreme_rows_non_empty(self, series_df):
        result = run_single_lake_service(series_df, hylak_id=1)
        rows = result_to_extreme_rows(result)
        assert len(rows) > 0
        for row in rows:
            assert "event_type" in row
            assert row["event_type"] in ("high", "low")
            assert "severity" in row


class TestResultToTransitionRows:
    def test_transitions_valid(self, series_df):
        result = run_single_lake_service(series_df, hylak_id=1)
        rows = result_to_transition_rows(result)
        for row in rows:
            assert "transition_type" in row
            assert row["transition_type"] in ("low_to_high", "high_to_low")


# ------------------------------------------------------------------
# Detection accuracy — known ground truth
# ------------------------------------------------------------------


def _make_normal_series(
    start_year: int = 2000,
    num_years: int = 25,
    *,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate a clean monthly lake area series with seasonal pattern.

    Returns a DataFrame with columns year, month, water_area.
    Noise is small enough that without inserted anomalies, few or no
    extremes should be detected.
    """
    rng = np.random.default_rng(seed)
    records = []
    for year in range(start_year, start_year + num_years):
        for month in range(1, 13):
            base = 100.0 + 20.0 * np.sin(2 * np.pi * month / 12)
            area = base * rng.uniform(0.92, 1.08)
            records.append({"year": year, "month": month, "water_area": area})
    return pd.DataFrame(records)


@pytest.fixture()
def normal_series():
    """Clean lake series with seasonal pattern and tight noise."""
    return _make_normal_series()


def _insert_anomalies(
    df: pd.DataFrame,
    anomalies: list[tuple[int, int, float]],
) -> pd.DataFrame:
    """Insert water_area anomalies at given (year, month, value)."""
    df = df.copy()
    for year, month, value in anomalies:
        mask = (df["year"] == year) & (df["month"] == month)
        df.loc[mask, "water_area"] = value
    return df


class TestPWMDetectionAccuracy:
    """Verify PWM extreme detection against known ground truth."""

    def test_detects_inserted_high_anomalies(self, normal_series):
        anomalies = [(2005, 7, 500.0), (2011, 3, 500.0), (2020, 11, 500.0)]
        df = _insert_anomalies(normal_series, anomalies)
        result = run_single_lake_service(df, hylak_id=1)
        labels = result.labels_df

        detected_high = labels.loc[labels["extreme_label"] == "extreme_high"]
        detected_set = set(
            zip(detected_high["year"].astype(int), detected_high["month"].astype(int))
        )
        for y, m, _ in anomalies:
            assert (y, m) in detected_set, (
                f"Anomaly ({y}, {m}) not detected as extreme_high"
            )

    def test_detects_inserted_low_anomalies(self, normal_series):
        anomalies = [(2005, 1, 5.0), (2012, 8, 5.0), (2019, 4, 5.0)]
        df = _insert_anomalies(normal_series, anomalies)
        result = run_single_lake_service(df, hylak_id=1)
        labels = result.labels_df

        detected_low = labels.loc[labels["extreme_label"] == "extreme_low"]
        detected_set = set(
            zip(detected_low["year"].astype(int), detected_low["month"].astype(int))
        )
        for y, m, _ in anomalies:
            assert (y, m) in detected_set, (
                f"Anomaly ({y}, {m}) not detected as extreme_low"
            )

    def test_few_false_positives_on_clean_series(self, normal_series):
        result = run_single_lake_service(normal_series, hylak_id=1)
        labels = result.labels_df
        extremes = labels.loc[labels["extreme_label"] != "normal"]
        n_total = len(normal_series)
        assert len(extremes) <= 0.30 * n_total, (
            f"Clean series should have ≤30% extremes, got {len(extremes)}/{n_total}"
        )
        assert normal_series["year"].nunique() >= 20

    def test_threshold_high_gt_low_all_months(self, normal_series):
        result = run_single_lake_service(normal_series, hylak_id=1)
        t_df = result.thresholds_df
        assert len(t_df) == 12
        for _, row in t_df.iterrows():
            assert row["threshold_high"] > row["threshold_low"], (
                f"month {int(row['month'])}: high={row['threshold_high']} <= low={row['threshold_low']}"
            )

    def test_thresholds_in_percentile_range(self, normal_series):
        result = run_single_lake_service(normal_series, hylak_id=1)
        t_df = result.thresholds_df
        for _, row in t_df.iterrows():
            assert 50 < row["threshold_high"] < 100, (
                f"month {int(row['month'])}: threshold_high={row['threshold_high']} out of (50, 100)"
            )
            assert 0 < row["threshold_low"] < 50, (
                f"month {int(row['month'])}: threshold_low={row['threshold_low']} out of (0, 50)"
            )

    def test_extreme_events_have_severity(self, normal_series):
        anomalies = [(2005, 7, 500.0)]
        df = _insert_anomalies(normal_series, anomalies)
        result = run_single_lake_service(df, hylak_id=1)
        extremes = result_to_extreme_rows(result)

        assert len(extremes) >= 1
        high_events = [r for r in extremes if r["event_type"] == "high"]
        assert len(high_events) >= 1
        for event in high_events:
            assert "severity" in event
            assert event["severity"] > 0

    def test_result_has_complete_labels(self, normal_series):
        result = run_single_lake_service(normal_series, hylak_id=1)
        rows = result_to_label_rows(result)
        labels_set = {row["extreme_label"] for row in rows}
        assert labels_set.issubset({"extreme_low", "extreme_high", "normal"})
        assert "normal" in labels_set
        assert len(rows) == len(normal_series)
