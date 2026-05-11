import numpy as np
import pandas as pd
import pytest

from lakeanalysis.quality.frozen import (
    FrozenPlateauSchedule,
    apply_frozen_plateau,
    build_frozen_plateau_schedule,
    defrozen_frame,
    filter_frozen_rows,
    first_frozen_months,
    frozen_run_indices,
    month_index_to_year_month_key,
    year_month_key_to_index,
    year_month_to_key,
)


class TestYearMonthKey:
    def test_jan_2000(self):
        assert year_month_to_key(2000, 1) == 200001

    def test_dec_2023(self):
        assert year_month_to_key(2023, 12) == 202312

    def test_roundtrip(self):
        for year in range(2000, 2005):
            for month in range(1, 13):
                key = year_month_to_key(year, month)
                recovered_year = key // 100
                recovered_month = key % 100
                assert recovered_year == year
                assert recovered_month == month


class TestKeyIndexConversion:
    def test_key_to_index(self):
        assert year_month_key_to_index(200001) == 2000 * 12 + 0
        assert year_month_key_to_index(200012) == 2000 * 12 + 11
        assert year_month_key_to_index(200101) == 2001 * 12 + 0

    def test_index_to_key(self):
        assert month_index_to_year_month_key(2000 * 12 + 0) == 200001
        assert month_index_to_year_month_key(2000 * 12 + 11) == 200012
        assert month_index_to_year_month_key(2001 * 12 + 0) == 200101

    def test_roundtrip(self):
        for key in (200001, 200006, 200012, 202312):
            idx = year_month_key_to_index(key)
            recovered = month_index_to_year_month_key(idx)
            assert recovered == key


class TestFrozenRunIndices:
    def test_empty_returns_empty(self):
        assert frozen_run_indices(None) == []
        assert frozen_run_indices(set()) == []

    def test_single_month(self):
        runs = frozen_run_indices({200001})
        assert runs == [(2000 * 12 + 0, 2000 * 12 + 0)]

    def test_contiguous_run(self):
        runs = frozen_run_indices({200001, 200002, 200003})
        assert len(runs) == 1
        assert runs[0] == (2000 * 12 + 0, 2000 * 12 + 2)

    def test_two_separate_runs(self):
        runs = frozen_run_indices({200001, 200002, 200004, 200005})
        assert len(runs) == 2
        assert runs[0] == (2000 * 12 + 0, 2000 * 12 + 1)
        assert runs[1] == (2000 * 12 + 3, 2000 * 12 + 4)


class TestFirstFrozenMonths:
    def test_single_month(self):
        first = first_frozen_months({200001})
        assert first == {200001}

    def test_contiguous_run_returns_first(self):
        first = first_frozen_months({200001, 200002, 200003})
        assert first == {200001}

    def test_two_runs_return_two_firsts(self):
        first = first_frozen_months({200001, 200002, 200004, 200005})
        assert first == {200001, 200004}


class TestBuildFrozenPlateauSchedule:
    def test_none_returns_none(self):
        assert build_frozen_plateau_schedule(None, 2000) is None

    def test_empty_returns_none(self):
        assert build_frozen_plateau_schedule(set(), 2000) is None

    def test_single_month_returns_schedule(self):
        schedule = build_frozen_plateau_schedule({200001}, 2000)
        assert schedule is not None
        assert len(schedule.anchor_times) == 1
        assert schedule.anchor_times[0] == pytest.approx(0.0)
        assert schedule.end_times[0] == pytest.approx(1.0 / 12.0)

    def test_contiguous_run(self):
        schedule = build_frozen_plateau_schedule({200001, 200002, 200003}, 2000)
        assert schedule is not None
        assert len(schedule.anchor_times) == 1
        assert schedule.anchor_times[0] == pytest.approx(0.0)
        assert schedule.end_times[0] == pytest.approx(3.0 / 12.0)


class TestApplyFrozenPlateau:
    def test_none_schedule_returns_unchanged(self):
        times = np.array([0.0, 0.1, 0.2])
        values = np.array([1.0, 2.0, 3.0])
        result = apply_frozen_plateau(times, values, None, None)
        np.testing.assert_array_equal(result, values)

    def test_applies_plateau_to_range(self):
        schedule = FrozenPlateauSchedule(
            anchor_times=np.array([0.0]),
            end_times=np.array([0.2]),
        )
        times = np.array([0.0, 0.1, 0.3])
        values = np.array([5.0, 6.0, 7.0])
        anchor_values = np.array([100.0])
        result = apply_frozen_plateau(times, values, schedule, anchor_values)
        assert result[0] == pytest.approx(100.0)
        assert result[1] == pytest.approx(100.0)
        assert result[2] == pytest.approx(7.0)


class TestFilterFrozenRows:
    @staticmethod
    def _make_frame(years=1):
        rows = []
        for year in range(2000, 2000 + years):
            for month in range(1, 13):
                rows.append({"year": year, "month": month, "water_area": 100.0})
        return pd.DataFrame(rows)

    def test_none_returns_unchanged(self):
        df = self._make_frame()
        result = filter_frozen_rows(df, None)
        assert len(result) == len(df)

    def test_empty_set_returns_unchanged(self):
        df = self._make_frame()
        result = filter_frozen_rows(df, set())
        assert len(result) == len(df)

    def test_removes_frozen_months(self):
        df = self._make_frame()
        frozen = {200001, 200006, 200012}
        result = filter_frozen_rows(df, frozen)
        assert len(result) == 9
        remaining = set(
            zip(result["year"].astype(int), result["month"].astype(int))
        )
        assert (2000, 1) not in remaining
        assert (2000, 6) not in remaining
        assert (2000, 12) not in remaining


class TestDefrozenFrame:
    @staticmethod
    def _make_frame(years=1):
        rows = []
        for year in range(2000, 2000 + years):
            for month in range(1, 13):
                rows.append({"year": year, "month": month, "water_area": 100.0 + month})
        return pd.DataFrame(rows)

    def test_none_returns_unchanged(self):
        df = self._make_frame()
        result = defrozen_frame(df, None)
        assert len(result) == len(df)

    def test_keeps_anchor_of_contiguous_run(self):
        df = self._make_frame()
        frozen = {200001, 200002, 200003}
        result = defrozen_frame(df, frozen)
        remaining = set(
            zip(result["year"].astype(int), result["month"].astype(int))
        )
        assert (2000, 1) in remaining
        assert (2000, 2) not in remaining
        assert (2000, 3) not in remaining

    def test_keeps_anchor_of_each_run(self):
        df = self._make_frame()
        frozen = {200001, 200002, 200004, 200005}
        result = defrozen_frame(df, frozen)
        remaining = set(
            zip(result["year"].astype(int), result["month"].astype(int))
        )
        assert (2000, 1) in remaining
        assert (2000, 4) in remaining
        assert (2000, 2) not in remaining
        assert (2000, 5) not in remaining
