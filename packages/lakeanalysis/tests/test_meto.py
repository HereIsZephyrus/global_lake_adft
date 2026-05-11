import numpy as np
import pandas as pd
import pytest

from lakeanalysis.meto.time import (
    continuous_time_from_year_month,
    normalize_monthly_index,
)
from lakeanalysis.meto.daily_aggregate import aggregate_daily_meteo_to_monthly
from lakeanalysis.meto.preprocess import (
    preprocess_meteo_export,
    validate_meteo_export_columns,
)
from lakeanalysis.meto.align import align_meteo_to_lake_monthly


# ─── meto/time.py ───────────────────────────────────────────────────────────


class TestContinuousTimeFromYearMonth:
    def test_scalar_origin(self):
        t = continuous_time_from_year_month(2000, 1, 2000)
        assert t == pytest.approx(0.0)

    def test_scalar_mid_year(self):
        t = continuous_time_from_year_month(2000, 7, 2000)
        assert t == pytest.approx(6 / 12)

    def test_scalar_next_year(self):
        t = continuous_time_from_year_month(2001, 1, 2000)
        assert t == pytest.approx(1.0)

    def test_scalar_dec(self):
        t = continuous_time_from_year_month(2000, 12, 2000)
        assert t == pytest.approx(11 / 12)

    def test_array_input(self):
        years = np.array([2000, 2000, 2001])
        months = np.array([1, 7, 1])
        result = continuous_time_from_year_month(years, months, 2000)
        expected = np.array([0.0, 6 / 12, 1.0])
        np.testing.assert_allclose(result, expected)

    def test_series_input(self):
        years = pd.Series([2000, 2001])
        months = pd.Series([1, 1])
        result = continuous_time_from_year_month(years, months, 2000)
        np.testing.assert_allclose(result, [0.0, 1.0])

    def test_different_start_year(self):
        t = continuous_time_from_year_month(2005, 1, 2000)
        assert t == pytest.approx(5.0)


class TestNormalizeMonthlyIndex:
    def test_basic(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2001],
            "month": [1, 6, 1],
            "value": [10, 20, 30],
        })
        result = normalize_monthly_index(df, hylak_col=None)
        assert "time" in result.columns
        assert result.iloc[0]["time"] == pytest.approx(0.0)
        assert result.iloc[1]["time"] == pytest.approx(5 / 12)
        assert result.iloc[2]["time"] == pytest.approx(1.0)

    def test_explicit_start_year(self):
        df = pd.DataFrame({"year": [2005], "month": [1], "val": [1]})
        result = normalize_monthly_index(df, start_year=2000, hylak_col=None)
        assert result.iloc[0]["time"] == pytest.approx(5.0)

    def test_sorts_by_year_month(self):
        df = pd.DataFrame({
            "year": [2001, 2000, 2000],
            "month": [1, 12, 6],
            "val": [3, 2, 1],
        })
        result = normalize_monthly_index(df, hylak_col=None)
        assert list(result["year"]) == [2000, 2000, 2001]
        assert list(result["month"]) == [6, 12, 1]

    def test_sorts_by_hylak_then_year_month(self):
        df = pd.DataFrame({
            "hylak_id": [2, 1, 1],
            "year": [2000, 2001, 2000],
            "month": [1, 1, 6],
            "val": [10, 30, 20],
        })
        result = normalize_monthly_index(df)
        assert list(result["hylak_id"]) == [1, 1, 2]
        assert list(result["year"]) == [2000, 2001, 2000]

    def test_invalid_month_raises(self):
        df = pd.DataFrame({"year": [2000], "month": [13], "val": [1]})
        with pytest.raises(ValueError, match="month must be in 1..12"):
            normalize_monthly_index(df, hylak_col=None)

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"year": [2000], "val": [1]})
        with pytest.raises(ValueError):
            normalize_monthly_index(df, hylak_col=None)

    def test_preserves_other_columns(self):
        df = pd.DataFrame({
            "year": [2000],
            "month": [1],
            "extra": ["hello"],
        })
        result = normalize_monthly_index(df, hylak_col=None)
        assert "extra" in result.columns
        assert result.iloc[0]["extra"] == "hello"


# ─── meto/daily_aggregate.py ────────────────────────────────────────────────


class TestAggregateDailyMeteoToMonthly:
    def test_sum_single_lake_single_month(self):
        df = pd.DataFrame({
            "date": pd.date_range("2000-01-01", periods=31, freq="D"),
            "hylak_id": [1] * 31,
            "precip": [1.0] * 31,
        })
        result = aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])
        assert len(result) == 1
        assert result.iloc[0]["precip"] == pytest.approx(31.0)
        assert result.iloc[0]["year"] == 2000
        assert result.iloc[0]["month"] == 1

    def test_mean_single_lake_single_month(self):
        df = pd.DataFrame({
            "date": pd.date_range("2000-01-01", periods=31, freq="D"),
            "hylak_id": [1] * 31,
            "temp": list(range(31)),
        })
        result = aggregate_daily_meteo_to_monthly(df, mean_columns=["temp"])
        assert len(result) == 1
        assert result.iloc[0]["temp"] == pytest.approx(15.0)

    def test_multiple_months(self):
        dates = pd.date_range("2000-01-01", periods=60, freq="D")
        df = pd.DataFrame({
            "date": dates,
            "hylak_id": [1] * 60,
            "precip": [1.0] * 60,
        })
        result = aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])
        assert len(result) == 2
        assert result.iloc[0]["month"] == 1
        assert result.iloc[1]["month"] == 2

    def test_multiple_lakes(self):
        dates = pd.date_range("2000-01-01", periods=10, freq="D")
        df = pd.DataFrame({
            "date": list(dates) * 2,
            "hylak_id": [1] * 10 + [2] * 10,
            "precip": [1.0] * 20,
        })
        result = aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])
        assert len(result) == 2
        assert set(result["hylak_id"]) == {1, 2}

    def test_sum_and_mean_together(self):
        df = pd.DataFrame({
            "date": pd.date_range("2000-01-01", periods=10, freq="D"),
            "hylak_id": [1] * 10,
            "precip": [2.0] * 10,
            "temp": [10.0] * 10,
        })
        result = aggregate_daily_meteo_to_monthly(
            df, sum_columns=["precip"], mean_columns=["temp"]
        )
        assert result.iloc[0]["precip"] == pytest.approx(20.0)
        assert result.iloc[0]["temp"] == pytest.approx(10.0)

    def test_missing_date_col_raises(self):
        df = pd.DataFrame({"hylak_id": [1], "precip": [1.0]})
        with pytest.raises(ValueError, match="Missing columns"):
            aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])

    def test_missing_hylak_col_raises(self):
        df = pd.DataFrame({"date": ["2000-01-01"], "precip": [1.0]})
        with pytest.raises(ValueError, match="Missing columns"):
            aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])

    def test_missing_value_col_raises(self):
        df = pd.DataFrame({
            "date": ["2000-01-01"],
            "hylak_id": [1],
        })
        with pytest.raises(ValueError, match="not in DataFrame"):
            aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])

    def test_no_agg_columns_raises(self):
        df = pd.DataFrame({
            "date": ["2000-01-01"],
            "hylak_id": [1],
            "precip": [1.0],
        })
        with pytest.raises(ValueError, match="At least one"):
            aggregate_daily_meteo_to_monthly(df)

    def test_invalid_date_raises(self):
        df = pd.DataFrame({
            "date": ["not-a-date"],
            "hylak_id": [1],
            "precip": [1.0],
        })
        with pytest.raises(ValueError, match="Invalid dates"):
            aggregate_daily_meteo_to_monthly(df, sum_columns=["precip"])

    def test_custom_column_names(self):
        df = pd.DataFrame({
            "dt": pd.date_range("2000-01-01", periods=5, freq="D"),
            "lake": [1] * 5,
            "rain": [2.0] * 5,
        })
        result = aggregate_daily_meteo_to_monthly(
            df, date_col="dt", hylak_col="lake", sum_columns=["rain"]
        )
        assert "lake" in result.columns
        assert result.iloc[0]["rain"] == pytest.approx(10.0)


# ─── meto/preprocess.py ─────────────────────────────────────────────────────


class TestValidateMeteoExportColumns:
    def test_all_present(self):
        df = pd.DataFrame({"hylak_id": [1], "year": [2000], "month": [1]})
        validate_meteo_export_columns(df, ["hylak_id", "year", "month"])

    def test_missing_hylak_raises(self):
        df = pd.DataFrame({"year": [2000], "month": [1]})
        with pytest.raises(ValueError, match="hylak_id"):
            validate_meteo_export_columns(df, ["hylak_id", "year"])

    def test_missing_other_raises(self):
        df = pd.DataFrame({"hylak_id": [1]})
        with pytest.raises(ValueError, match="Missing required columns"):
            validate_meteo_export_columns(df, ["hylak_id", "year", "month"])


class TestPreprocessMeteoExport:
    def test_basic_sort_and_cast(self):
        df = pd.DataFrame({
            "hylak_id": [2.0, 1.0, 1.0],
            "year": [2000.0, 2001.0, 2000.0],
            "month": [1.0, 1.0, 6.0],
            "temp": [10.0, 30.0, 20.0],
        })
        result = preprocess_meteo_export(df)
        assert result["hylak_id"].dtype == int
        assert result["year"].dtype == int
        assert result["month"].dtype == int
        assert list(result["hylak_id"]) == [1, 1, 2]
        assert list(result["year"]) == [2000, 2001, 2000]

    def test_drops_na_keys(self):
        df = pd.DataFrame({
            "hylak_id": [1, None, 2],
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "val": [10, 20, 30],
        })
        result = preprocess_meteo_export(df)
        assert len(result) == 2

    def test_drops_all_na_feature_cols(self):
        df = pd.DataFrame({
            "hylak_id": [1, 2],
            "year": [2000, 2000],
            "month": [1, 2],
            "good": [1.0, 2.0],
            "bad": [None, None],
        })
        result = preprocess_meteo_export(df)
        assert "good" in result.columns
        assert "bad" not in result.columns

    def test_keep_all_na_feature_cols_when_disabled(self):
        df = pd.DataFrame({
            "hylak_id": [1],
            "year": [2000],
            "month": [1],
            "bad": [None],
        })
        result = preprocess_meteo_export(df, drop_all_na_feature_cols=False)
        assert "bad" in result.columns

    def test_validates_required_columns(self):
        df = pd.DataFrame({
            "hylak_id": [1],
            "year": [2000],
            "month": [1],
        })
        with pytest.raises(ValueError, match="Missing"):
            preprocess_meteo_export(df, required_columns=["hylak_id", "year", "month", "temp"])

    def test_missing_key_cols_raises(self):
        df = pd.DataFrame({"hylak_id": [1], "year": [2000]})
        with pytest.raises(ValueError, match="must include"):
            preprocess_meteo_export(df)

    def test_empty_after_dropna(self):
        df = pd.DataFrame({
            "hylak_id": [None],
            "year": [None],
            "month": [None],
        })
        result = preprocess_meteo_export(df)
        assert result.empty


# ─── meto/align.py ──────────────────────────────────────────────────────────


class TestAlignMeteoToLakeMonthly:
    def test_basic_left_join(self):
        lake = pd.DataFrame({
            "hylak_id": [1, 1, 1],
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "water_area": [100.0, 110.0, 120.0],
        })
        meteo = pd.DataFrame({
            "hylak_id": [1, 1],
            "year": [2000, 2000],
            "month": [1, 2],
            "temp": [5.0, 7.0],
        })
        result = align_meteo_to_lake_monthly(lake, meteo)
        assert len(result) == 3
        assert result.iloc[0]["temp"] == 5.0
        assert result.iloc[1]["temp"] == 7.0
        assert pd.isna(result.iloc[2]["temp"])

    def test_time_column_present(self):
        lake = pd.DataFrame({
            "hylak_id": [1],
            "year": [2000],
            "month": [1],
            "area": [100.0],
        })
        meteo = pd.DataFrame({
            "hylak_id": [1],
            "year": [2000],
            "month": [1],
            "temp": [5.0],
        })
        result = align_meteo_to_lake_monthly(lake, meteo)
        assert "time" in result.columns
        assert result.iloc[0]["time"] == pytest.approx(0.0)

    def test_explicit_start_year(self):
        lake = pd.DataFrame({
            "hylak_id": [1],
            "year": [2005],
            "month": [1],
            "area": [100.0],
        })
        meteo = pd.DataFrame({
            "hylak_id": [1],
            "year": [2005],
            "month": [1],
            "temp": [5.0],
        })
        result = align_meteo_to_lake_monthly(lake, meteo, start_year=2000)
        assert result.iloc[0]["time"] == pytest.approx(5.0)

    def test_drops_meteo_duplicates(self):
        lake = pd.DataFrame({
            "hylak_id": [1],
            "year": [2000],
            "month": [1],
            "area": [100.0],
        })
        meteo = pd.DataFrame({
            "hylak_id": [1, 1],
            "year": [2000, 2000],
            "month": [1, 1],
            "temp": [5.0, 99.0],
        })
        result = align_meteo_to_lake_monthly(lake, meteo)
        assert len(result) == 1
        assert result.iloc[0]["temp"] == 5.0

    def test_missing_column_raises(self):
        lake = pd.DataFrame({"hylak_id": [1], "year": [2000], "month": [1]})
        meteo = pd.DataFrame({"hylak_id": [1], "year": [2000]})
        with pytest.raises(ValueError, match="missing column"):
            align_meteo_to_lake_monthly(lake, meteo)

    def test_multiple_lakes(self):
        lake = pd.DataFrame({
            "hylak_id": [1, 2],
            "year": [2000, 2000],
            "month": [1, 1],
            "area": [100.0, 200.0],
        })
        meteo = pd.DataFrame({
            "hylak_id": [1, 2],
            "year": [2000, 2000],
            "month": [1, 1],
            "temp": [5.0, 8.0],
        })
        result = align_meteo_to_lake_monthly(lake, meteo)
        assert len(result) == 2
