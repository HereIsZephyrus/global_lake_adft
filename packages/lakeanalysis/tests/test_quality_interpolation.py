import numpy as np
import pandas as pd
import pytest

from lakeanalysis.quality.interpolation import (
    CollinearSegment,
    InterpolationConfig,
    InterpolationResult,
    _adjacent_mask,
    _month_index,
    _prepare_series,
    _year_month_key,
    detect_interpolation,
    get_collinear_segments,
)


class TestYearMonthKey:
    def test_jan_2000(self):
        assert _year_month_key(2000, 1) == 200001

    def test_dec_2023(self):
        assert _year_month_key(2023, 12) == 202312


class TestMonthIndex:
    def test_jan_2000(self):
        assert _month_index(2000, 1) == 2000 * 12

    def test_dec_2000(self):
        assert _month_index(2000, 12) == 2000 * 12 + 11

    def test_jan_2001(self):
        assert _month_index(2001, 1) == 2001 * 12


class TestInterpolationConfig:
    def test_defaults(self):
        cfg = InterpolationConfig()
        assert cfg.rtol == 0.0
        assert cfg.atol == 1e-6
        assert cfg.min_collinear_points == 4

    def test_custom(self):
        cfg = InterpolationConfig(rtol=1e-3, atol=1e-2, min_collinear_points=5)
        assert cfg.rtol == 1e-3
        assert cfg.atol == 1e-2
        assert cfg.min_collinear_points == 5

    def test_frozen(self):
        cfg = InterpolationConfig()
        with pytest.raises(Exception):
            cfg.rtol = 0.1


class TestInterpolationResult:
    def test_all_fields(self):
        r = InterpolationResult(
            has_interpolation=True,
            n_linear_segments=2,
            n_flat_segments=1,
            max_linear_len=5,
            max_flat_len=4,
            collinear_ratio=0.3,
            first_linear_ym=200001,
            n_obs=20,
        )
        assert r.has_interpolation is True
        assert r.n_linear_segments == 2
        assert r.n_flat_segments == 1
        assert r.max_linear_len == 5
        assert r.max_flat_len == 4
        assert r.collinear_ratio == pytest.approx(0.3)
        assert r.first_linear_ym == 200001
        assert r.n_obs == 20


class TestCollinearSegment:
    def test_fields(self):
        seg = CollinearSegment(
            start_idx=2,
            end_idx=6,
            length=5,
            is_flat=False,
            diff_value=3.5,
            start_ym=200003,
        )
        assert seg.start_idx == 2
        assert seg.end_idx == 6
        assert seg.length == 5
        assert seg.is_flat is False
        assert seg.diff_value == 3.5
        assert seg.start_ym == 200003


class TestPrepareSeries:
    def test_no_frozen_no_zeros(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "water_area": [10.0, 12.0, 14.0],
        })
        result = _prepare_series(df)
        assert len(result) == 3

    def test_filters_zero_area(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "water_area": [10.0, 0.0, 14.0],
        })
        result = _prepare_series(df)
        assert len(result) == 2
        assert 0.0 not in result["water_area"].to_numpy()

    def test_filters_frozen_months(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4],
            "water_area": [10.0, 12.0, 14.0, 16.0],
        })
        result = _prepare_series(df, frozen_year_months={200002, 200004})
        assert len(result) == 2
        assert 2 not in result["month"].to_numpy()
        assert 4 not in result["month"].to_numpy()

    def test_frozen_and_zeros_combined(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4],
            "water_area": [10.0, 0.0, 14.0, 16.0],
        })
        result = _prepare_series(df, frozen_year_months={200003})
        assert len(result) == 2
        assert list(result["month"]) == [1, 4]

    def test_sorts_by_year_month(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 1999],
            "month": [3, 1, 12],
            "water_area": [14.0, 10.0, 20.0],
        })
        result = _prepare_series(df)
        assert list(result["year"]) == [1999, 2000, 2000]
        assert list(result["month"]) == [12, 1, 3]

    def test_empty_after_filtering(self):
        df = pd.DataFrame({
            "year": [2000, 2000],
            "month": [1, 2],
            "water_area": [0.0, 0.0],
        })
        result = _prepare_series(df)
        assert len(result) == 0

    def test_preserves_other_columns(self):
        df = pd.DataFrame({
            "year": [2000, 2000],
            "month": [1, 2],
            "water_area": [10.0, 12.0],
            "extra": ["a", "b"],
        })
        result = _prepare_series(df)
        assert list(result["extra"]) == ["a", "b"]


class TestAdjacentMask:
    def test_all_consecutive(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2000, 2000],
            "month": [1, 2, 3, 4],
            "water_area": [1.0, 2.0, 3.0, 4.0],
        })
        mask = _adjacent_mask(df)
        assert mask.tolist() == [True, True, True]

    def test_month_gap(self):
        df = pd.DataFrame({
            "year": [2000, 2000, 2000],
            "month": [1, 2, 4],
            "water_area": [1.0, 2.0, 4.0],
        })
        mask = _adjacent_mask(df)
        assert mask.tolist() == [True, False]

    def test_year_gap(self):
        df = pd.DataFrame({
            "year": [2000, 2001, 2001],
            "month": [12, 1, 1],
            "water_area": [1.0, 2.0, 3.0],
        })
        mask = _adjacent_mask(df)
        assert mask.tolist() == [True, False]

    def test_year_boundary_consecutive(self):
        df = pd.DataFrame({
            "year": [2000, 2001],
            "month": [12, 1],
            "water_area": [1.0, 2.0],
        })
        mask = _adjacent_mask(df)
        assert mask.tolist() == [True]

    def test_two_rows(self):
        df = pd.DataFrame({
            "year": [2000, 2000],
            "month": [1, 2],
            "water_area": [1.0, 2.0],
        })
        mask = _adjacent_mask(df)
        assert mask.tolist() == [True]


class TestDetectInterpolation:
    @staticmethod
    def _make_df(years, months, areas):
        return pd.DataFrame({
            "year": years,
            "month": months,
            "water_area": areas,
        })

    def test_too_few_observations(self):
        df = self._make_df(
            [2000, 2000, 2000], [1, 2, 3], [10.0, 12.0, 14.0]
        )
        result = detect_interpolation(df)
        assert not result.has_interpolation
        assert result.n_linear_segments == 0
        assert result.n_flat_segments == 0
        assert result.max_linear_len == 0
        assert result.max_flat_len == 0
        assert result.collinear_ratio == 0.0
        assert result.first_linear_ym is None
        assert result.n_obs == 3

    def test_exactly_min_collinear_no_interpolation(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [10.0, 12.0, 14.0, 16.0],
        )
        result = detect_interpolation(df)
        assert result.has_interpolation

    def test_random_data(self):
        rng = np.random.default_rng(42)
        months = list(range(1, 25))
        areas = rng.uniform(100, 200, size=24)
        years = [2000 + (m - 1) // 12 for m in months]
        months_1to12 = [(m - 1) % 12 + 1 for m in months]
        df = self._make_df(years, months_1to12, areas)
        result = detect_interpolation(df)
        assert not result.has_interpolation

    def test_perfect_linear_five_points(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5],
            [10.0, 12.0, 14.0, 16.0, 18.0],
        )
        result = detect_interpolation(df)
        assert result.has_interpolation
        assert result.n_linear_segments == 1
        assert result.n_flat_segments == 0
        assert result.max_linear_len == 5
        assert result.first_linear_ym == 200001

    def test_flat_four_points(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [10.0, 10.0, 10.0, 10.0],
        )
        result = detect_interpolation(df)
        assert result.has_interpolation
        assert result.n_linear_segments == 0
        assert result.n_flat_segments == 1
        assert result.max_flat_len == 4

    def test_linear_and_flat_segments(self):
        years = [2000] * 10
        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        areas = [
            10.0, 12.0, 14.0, 16.0, 18.0,  # linear (5 pts)
            50.0,                             # break
            20.0, 20.0, 20.0, 20.0,          # flat (4 pts)
        ]
        df = self._make_df(years, months, areas)
        result = detect_interpolation(df)
        assert result.has_interpolation
        assert result.n_linear_segments == 1
        assert result.n_flat_segments == 1
        assert result.max_linear_len == 5
        assert result.max_flat_len == 4

    def test_gap_breaks_collinearity(self):
        years = [2000, 2000, 2000, 2000, 2000]
        months = [1, 2, 3, 5, 6]
        areas = [10.0, 12.0, 14.0, 16.0, 18.0]
        df = self._make_df(years, months, areas)
        result = detect_interpolation(df)
        assert not result.has_interpolation

    def test_zero_area_excluded_breaks_collinearity(self):
        years = [2000, 2000, 2000, 2000, 2000]
        months = [1, 2, 3, 4, 5]
        areas = [10.0, 12.0, 0.0, 16.0, 18.0]
        df = self._make_df(years, months, areas)
        result = detect_interpolation(df)
        assert not result.has_interpolation

    def test_frozen_breaks_collinearity(self):
        years = [2000, 2000, 2000, 2000, 2000]
        months = [1, 2, 3, 4, 5]
        areas = [10.0, 12.0, 14.0, 16.0, 18.0]
        df = self._make_df(years, months, areas)
        result = detect_interpolation(df, frozen_year_months={200003})
        assert not result.has_interpolation

    def test_multiple_collinear_runs(self):
        years = [2000] * 10
        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        areas = [
            10.0, 12.0, 14.0, 16.0,  # run 1: linear 4 pts
            100.0,                      # break
            20.0, 20.0, 20.0, 20.0,  # run 2: flat 4 pts
            50.0,                      # break
        ]
        df = self._make_df(years, months, areas)
        result = detect_interpolation(df)
        assert result.has_interpolation
        assert result.n_linear_segments + result.n_flat_segments == 2

    def test_collinear_ratio(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5, 6],
            [10.0, 12.0, 14.0, 16.0, 100.0, 200.0],
        )
        result = detect_interpolation(df)
        assert result.collinear_ratio == pytest.approx(4 / 6)

    def test_first_linear_ym_set(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5, 6, 7, 8],
            [
                100.0,                       # break
                10.0, 12.0, 14.0, 16.0,     # linear at month 2
                50.0, 52.0, 54.0,            # not enough points
            ],
        )
        result = detect_interpolation(df)
        assert result.first_linear_ym == 200002

    def test_custom_min_collinear_points(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5],
            [10.0, 12.0, 14.0, 16.0, 18.0],
        )
        cfg = InterpolationConfig(min_collinear_points=6)
        result = detect_interpolation(df, config=cfg)
        assert not result.has_interpolation

    def test_custom_atol_flat_detection(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [10.0, 10.00001, 10.00002, 10.00003],
        )
        cfg = InterpolationConfig(atol=1e-2)
        result = detect_interpolation(df, config=cfg)
        assert result.has_interpolation
        assert result.n_flat_segments == 1

    def test_tight_atol_no_interpolation(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [10.0, 10.0001, 10.0002, 10.0003],
        )
        cfg = InterpolationConfig(atol=1e-6)
        result = detect_interpolation(df, config=cfg)
        assert result.has_interpolation

    def test_empty_df(self):
        df = pd.DataFrame(columns=["year", "month", "water_area"])
        result = detect_interpolation(df)
        assert not result.has_interpolation
        assert result.n_obs == 0

    def test_collinear_across_year_boundary(self):
        df = self._make_df(
            [2000, 2000, 2001, 2001],
            [11, 12, 1, 2],
            [10.0, 12.0, 14.0, 16.0],
        )
        result = detect_interpolation(df)
        assert result.has_interpolation
        assert result.n_linear_segments == 1

    def test_constant_but_below_min_points(self):
        df = self._make_df(
            [2000, 2000, 2000],
            [1, 2, 3],
            [5.0, 5.0, 5.0],
        )
        result = detect_interpolation(df)
        assert not result.has_interpolation

    def test_four_constant_points(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [5.0, 5.0, 5.0, 5.0],
        )
        result = detect_interpolation(df)
        assert result.has_interpolation
        assert result.n_flat_segments == 1
        assert result.max_flat_len == 4


class TestGetCollinearSegments:
    @staticmethod
    def _make_df(years, months, areas):
        return pd.DataFrame({
            "year": years,
            "month": months,
            "water_area": areas,
        })

    def test_no_segments(self):
        df = self._make_df(
            [2000, 2000, 2000], [1, 2, 3], [10.0, 12.0, 14.0]
        )
        segs = get_collinear_segments(df)
        assert segs == []

    def test_linear_segment(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5],
            [10.0, 12.0, 14.0, 16.0, 18.0],
        )
        segs = get_collinear_segments(df)
        assert len(segs) == 1
        seg = segs[0]
        assert seg.start_idx == 0
        assert seg.end_idx == 4
        assert seg.length == 5
        assert not seg.is_flat
        assert seg.diff_value == pytest.approx(2.0)
        assert seg.start_ym == 200001

    def test_flat_segment(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [1, 2, 3, 4],
            [10.0, 10.0, 10.0, 10.0],
        )
        segs = get_collinear_segments(df)
        assert len(segs) == 1
        seg = segs[0]
        assert seg.is_flat
        assert seg.diff_value == pytest.approx(0.0)

    def test_multiple_segments(self):
        df = self._make_df(
            [2000] * 10,
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            [
                10.0, 12.0, 14.0, 16.0,  # linear
                100.0,                    # break
                20.0, 20.0, 20.0, 20.0,  # flat
                50.0,                     # break
            ],
        )
        segs = get_collinear_segments(df)
        assert len(segs) == 2

    def test_with_frozen_months(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5, 6, 7],
            [10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0],
        )
        segs = get_collinear_segments(df, frozen_year_months={200003})
        months_left = [s.start_ym for s in segs]
        assert 200003 not in months_left

    def test_custom_config(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000, 2000],
            [1, 2, 3, 4, 5],
            [10.0, 12.0, 14.0, 16.0, 18.0],
        )
        cfg = InterpolationConfig(min_collinear_points=6)
        segs = get_collinear_segments(df, config=cfg)
        assert segs == []

    def test_empty_after_prepare(self):
        df = self._make_df(
            [2000, 2000], [1, 2], [0.0, 0.0]
        )
        segs = get_collinear_segments(df)
        assert segs == []

    def test_start_ym_accurate(self):
        df = self._make_df(
            [2000, 2000, 2000, 2000],
            [6, 7, 8, 9],
            [10.0, 12.0, 14.0, 16.0],
        )
        segs = get_collinear_segments(df)
        assert segs[0].start_ym == 200006
