"""Tests for export file name derivation."""

from __future__ import annotations

from datetime import date

from hydrofetch.export.namer import image_day_prefix, image_range_prefix, iter_daily_date_range


class TestImageDayPrefix:
    def test_basic(self):
        assert image_day_prefix("era5_land_daily_image", date(2020, 1, 15)) == (
            "era5_land_daily_image_20200115"
        )

    def test_from_iso_string(self):
        assert image_day_prefix("spec_id", "2021-06-01") == "spec_id_20210601"


class TestImageRangePrefix:
    def test_basic(self):
        result = image_range_prefix("era5", date(2020, 1, 1), date(2020, 2, 1))
        assert result == "era5_20200101_20200201"


class TestIterDailyDateRange:
    def test_yields_correct_dates(self):
        dates = list(iter_daily_date_range("2020-01-01", "2020-01-04"))
        assert dates == [date(2020, 1, 1), date(2020, 1, 2), date(2020, 1, 3)]

    def test_empty_when_start_equals_end(self):
        assert list(iter_daily_date_range("2020-01-01", "2020-01-01")) == []

    def test_single_day(self):
        assert list(iter_daily_date_range("2020-06-15", "2020-06-16")) == [date(2020, 6, 15)]
