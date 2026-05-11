import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.series import MIN_OBSERVATIONS, MonthlyTimeSeries, TailDirection


def _make_frame(years=5):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            rows.append(
                {"year": year, "month": month, "water_area": 100.0 + month + year % 2 * 5.0}
            )
    return pd.DataFrame(rows)


class TestMonthlyTimeSeriesFromFrame:
    def test_basic_construction(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        assert mts.n_obs == 60
        assert mts.value_column == "water_area"
        assert mts.direction == "high"

    def test_missing_required_columns_raises(self):
        df = pd.DataFrame({"year": [2000], "water_area": [100.0]})
        with pytest.raises(ValueError, match="Missing required columns"):
            MonthlyTimeSeries.from_frame(df)

    def test_empty_after_dropna_raises(self):
        df = pd.DataFrame({"year": [None], "month": [None], "water_area": [None]})
        with pytest.raises(ValueError, match="empty"):
            MonthlyTimeSeries.from_frame(df)

    def test_invalid_month_raises(self):
        df = pd.DataFrame([{"year": 2000, "month": 0, "water_area": 100.0}])
        with pytest.raises(ValueError, match="Month values must be in the range 1..12"):
            MonthlyTimeSeries.from_frame(df)

    def test_time_column_computed_correctly(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=1))
        assert mts.data.loc[0, "time"] == 0.0
        dec = mts.data.loc[mts.data["month"] == 12, "time"].iloc[0]
        assert dec == pytest.approx(11.0 / 12.0)

    def test_values_and_original_values_equal_for_high(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        np.testing.assert_array_equal(mts.values, mts.original_values)


class TestTailTransformation:
    def test_for_tail_same_direction_returns_self(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        result = mts.for_tail("high")
        assert result is mts

    def test_for_tail_low_negates_values(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        low = mts.for_tail("low")
        np.testing.assert_array_equal(low.values, -mts.values)

    def test_for_tail_roundtrip(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        low = mts.for_tail("low")
        high_again = low.for_tail("high")
        np.testing.assert_array_equal(high_again.values, mts.values)


class TestDefrozen:
    def test_none_frozen_returns_self(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        result = mts.defrozen(None)
        assert result is mts

    def test_empty_frozen_set_returns_self(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame())
        result = mts.defrozen(set())
        assert result is mts

    def test_frozen_months_removed(self):
        """defrozen_frame keeps the first month of each contiguous frozen run as anchor."""
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=1))
        frozen = {200001, 200002, 200012}
        result = mts.defrozen(frozen)
        remaining = set(
            zip(result.data["year"].astype(int), result.data["month"].astype(int))
        )
        assert (2000, 2) not in remaining

    def test_contiguous_frozen_run_removes_interior(self):
        """Months 2-4 frozen: month 2 kept as anchor, 3-4 removed."""
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=1))
        frozen = {200002, 200003, 200004}
        result = mts.defrozen(frozen)
        remaining = set(
            zip(result.data["year"].astype(int), result.data["month"].astype(int))
        )
        assert (2000, 3) not in remaining
        assert (2000, 4) not in remaining
        assert result.n_obs == 10


class TestDurationYears:
    def test_empty_frame_returns_zero(self):
        mts = MonthlyTimeSeries(
            data=pd.DataFrame(columns=["year", "month", "time", "value", "original_value"]),
        )
        assert mts.duration_years == 0.0

    def test_single_observation(self):
        df = pd.DataFrame([{"year": 2000, "month": 6, "water_area": 100.0}])
        mts = MonthlyTimeSeries.from_frame(df)
        assert mts.duration_years == pytest.approx(1.0 / 12.0)

    def test_five_year_span(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=5))
        assert mts.duration_years == pytest.approx(5.0)

    def test_n_obs_property(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=3))
        assert mts.n_obs == 36


class TestValidateMinObservations:
    def test_sufficient_observations_returns_self(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=2))
        result = mts.validate_min_observations(min_observations=10)
        assert result is mts

    def test_insufficient_observations_raises(self):
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=1))
        with pytest.raises(ValueError, match="At least"):
            mts.validate_min_observations(min_observations=100)

    def test_default_min_observations(self):
        """Two years of data gives 24 > 20, should pass."""
        mts = MonthlyTimeSeries.from_frame(_make_frame(years=2))
        assert mts.validate_min_observations().n_obs == 24
