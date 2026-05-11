import numpy as np
import pandas as pd
import pytest

from lakeanalysis.artificial.impact.events import (
    compute_event_stats,
    compute_pair_events,
    detect_zscore_events,
)
from lakeanalysis.artificial.impact.metrics import (
    compute_cv,
    compute_lake_metrics,
    compute_pair_metrics,
    compute_pct_change_std,
    compute_range_ratio,
)
from lakeanalysis.artificial.similarity.compute import (
    acf_cosine_similarity,
    align_series,
    compute_pair_similarity,
    pearson_correlation,
)


# ─── artificial/impact/events.py ────────────────────────────────────────────


class TestDetectZscoreEvents:
    def test_no_events_normal_data(self):
        df = pd.DataFrame({
            "year": [2000] * 20,
            "month": list(range(1, 13)) + list(range(1, 9)),
            "water_area": [100.0] * 20,
        })
        events = detect_zscore_events(df)
        assert events == []

    def test_detects_outlier(self):
        areas = [100.0] * 50 + [500.0]
        df = pd.DataFrame({
            "year": [2000 + i // 12 for i in range(51)],
            "month": [i % 12 + 1 for i in range(51)],
            "water_area": areas,
        })
        events = detect_zscore_events(df)
        assert len(events) >= 1
        assert events[0]["water_area"] == 500.0

    def test_custom_threshold(self):
        areas = [100.0] * 50 + [200.0]
        df = pd.DataFrame({
            "year": [2000 + i // 12 for i in range(51)],
            "month": [i % 12 + 1 for i in range(51)],
            "water_area": areas,
        })
        events_strict = detect_zscore_events(df, threshold=3.0)
        events_loose = detect_zscore_events(df, threshold=1.0)
        assert len(events_loose) >= len(events_strict)

    def test_empty_df(self):
        df = pd.DataFrame(columns=["year", "month", "water_area"])
        assert detect_zscore_events(df) == []

    def test_single_row(self):
        df = pd.DataFrame({"year": [2000], "month": [1], "water_area": [100.0]})
        assert detect_zscore_events(df) == []

    def test_constant_series(self):
        df = pd.DataFrame({
            "year": [2000] * 10,
            "month": list(range(1, 11)),
            "water_area": [5.0] * 10,
        })
        assert detect_zscore_events(df) == []


class TestComputeEventStats:
    def test_basic(self):
        areas = [100.0] * 50 + [500.0]
        df = pd.DataFrame({
            "year": [2000 + i // 12 for i in range(51)],
            "month": [i % 12 + 1 for i in range(51)],
            "water_area": areas,
        })
        stats = compute_event_stats(df)
        assert stats["n_obs"] == 51
        assert stats["n_events"] >= 1
        assert stats["anomaly_ratio"] == stats["n_events"] / 51

    def test_empty(self):
        df = pd.DataFrame(columns=["year", "month", "water_area"])
        stats = compute_event_stats(df)
        assert stats["n_events"] == 0
        assert np.isnan(stats["anomaly_ratio"])


class TestComputePairEvents:
    def test_unique_events(self):
        areas_a = [100.0] * 50 + [500.0]
        areas_n = [100.0] * 51
        n = 51
        df_a = pd.DataFrame({
            "year": [2000 + i // 12 for i in range(n)],
            "month": [i % 12 + 1 for i in range(n)],
            "water_area": areas_a,
        })
        df_n = pd.DataFrame({
            "year": [2000 + i // 12 for i in range(n)],
            "month": [i % 12 + 1 for i in range(n)],
            "water_area": areas_n,
        })
        result = compute_pair_events(df_a, df_n)
        assert result["n_events_a"] >= 1
        assert result["n_events_n"] == 0
        assert result["n_unique_a"] >= 1


# ─── artificial/impact/metrics.py ───────────────────────────────────────────


class TestComputeCv:
    def test_basic(self):
        series = np.array([10.0, 20.0, 30.0])
        cv = compute_cv(series)
        assert cv == pytest.approx(np.std(series) / np.mean(series))

    def test_constant(self):
        series = np.array([5.0, 5.0, 5.0])
        assert compute_cv(series) == pytest.approx(0.0)

    def test_zero_mean(self):
        series = np.array([0.0, 0.0, 0.0])
        assert np.isnan(compute_cv(series))

    def test_single_value(self):
        assert np.isnan(compute_cv(np.array([5.0])))


class TestComputePctChangeStd:
    def test_basic(self):
        series = np.array([100.0, 110.0, 121.0, 133.1])
        result = compute_pct_change_std(series)
        assert result > 0

    def test_constant(self):
        series = np.array([5.0, 5.0, 5.0, 5.0])
        assert compute_pct_change_std(series) == pytest.approx(0.0)

    def test_with_zero(self):
        series = np.array([0.0, 10.0, 20.0])
        result = compute_pct_change_std(series)
        assert np.isnan(result)

    def test_single_value(self):
        assert np.isnan(compute_pct_change_std(np.array([5.0])))


class TestComputeRangeRatio:
    def test_basic(self):
        series = np.array([10.0, 20.0, 30.0])
        assert compute_range_ratio(series) == pytest.approx(20.0 / 20.0)

    def test_constant(self):
        series = np.array([5.0, 5.0, 5.0])
        assert compute_range_ratio(series) == pytest.approx(0.0)

    def test_zero_mean(self):
        assert np.isnan(compute_range_ratio(np.array([0.0, 0.0])))

    def test_single_value(self):
        assert np.isnan(compute_range_ratio(np.array([5.0])))


class TestComputeLakeMetrics:
    def test_basic(self):
        df = pd.DataFrame({
            "year": [2000] * 10,
            "month": list(range(1, 11)),
            "water_area": [float(x) for x in range(10, 20)],
        })
        m = compute_lake_metrics(df)
        assert m["n_obs"] == 10
        assert not np.isnan(m["cv"])
        assert not np.isnan(m["range_ratio"])

    def test_empty(self):
        df = pd.DataFrame(columns=["year", "month", "water_area"])
        m = compute_lake_metrics(df)
        assert m["n_obs"] == 0
        assert np.isnan(m["cv"])


class TestComputePairMetrics:
    def test_basic(self):
        df_a = pd.DataFrame({
            "year": [2000] * 10,
            "month": list(range(1, 11)),
            "water_area": [float(x) for x in range(10, 20)],
        })
        df_n = pd.DataFrame({
            "year": [2000] * 10,
            "month": list(range(1, 11)),
            "water_area": [float(x) for x in range(20, 30)],
        })
        result = compute_pair_metrics(df_a, df_n)
        assert "cv_a" in result
        assert "cv_n" in result
        assert "delta_cv" in result
        assert result["delta_cv"] == pytest.approx(
            result["cv_a"] - result["cv_n"]
        )


# ─── artificial/similarity/compute.py ───────────────────────────────────────


class TestAlignSeries:
    def test_basic(self):
        df_a = pd.DataFrame({
            "year": [2000, 2000, 2000],
            "month": [1, 2, 3],
            "water_area": [10.0, 20.0, 30.0],
        })
        df_b = pd.DataFrame({
            "year": [2000, 2000],
            "month": [2, 3],
            "water_area": [25.0, 35.0],
        })
        arr_a, arr_b = align_series(df_a, df_b)
        assert len(arr_a) == 2
        np.testing.assert_array_equal(arr_a, [20.0, 30.0])
        np.testing.assert_array_equal(arr_b, [25.0, 35.0])

    def test_no_overlap(self):
        df_a = pd.DataFrame({
            "year": [2000], "month": [1], "water_area": [10.0]
        })
        df_b = pd.DataFrame({
            "year": [2001], "month": [1], "water_area": [20.0]
        })
        arr_a, arr_b = align_series(df_a, df_b)
        assert len(arr_a) == 0

    def test_empty_input(self):
        df_a = pd.DataFrame(columns=["year", "month", "water_area"])
        df_b = pd.DataFrame({
            "year": [2000], "month": [1], "water_area": [10.0]
        })
        arr_a, arr_b = align_series(df_a, df_b)
        assert len(arr_a) == 0


class TestPearsonCorrelation:
    def test_perfect_positive(self):
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([2.0, 4.0, 6.0, 8.0, 10.0])
        assert pearson_correlation(a, b) == pytest.approx(1.0)

    def test_perfect_negative(self):
        a = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        b = np.array([10.0, 8.0, 6.0, 4.0, 2.0])
        assert pearson_correlation(a, b) == pytest.approx(-1.0)

    def test_constant_returns_nan(self):
        a = np.array([5.0, 5.0, 5.0])
        b = np.array([1.0, 2.0, 3.0])
        assert np.isnan(pearson_correlation(a, b))

    def test_single_value_returns_nan(self):
        assert np.isnan(pearson_correlation(np.array([1.0]), np.array([2.0])))

    def test_different_lengths_returns_nan(self):
        assert np.isnan(
            pearson_correlation(np.array([1.0, 2.0]), np.array([1.0, 2.0, 3.0]))
        )


class TestAcfCosineSimilarity:
    def test_identical_series(self):
        rng = np.random.default_rng(42)
        series = np.sin(np.linspace(0, 4 * np.pi, 48)) + rng.normal(0, 0.1, 48)
        result = acf_cosine_similarity(series, series)
        assert result == pytest.approx(1.0, abs=1e-6)

    def test_insufficient_data(self):
        a = np.array([1.0, 2.0, 3.0])
        assert np.isnan(acf_cosine_similarity(a, a, period=12))

    def test_different_lengths(self):
        a = np.arange(50.0)
        b = np.arange(30.0)
        assert np.isnan(acf_cosine_similarity(a, b))

    def test_range(self):
        rng = np.random.default_rng(123)
        a = rng.normal(0, 1, 48)
        b = rng.normal(0, 1, 48)
        result = acf_cosine_similarity(a, b)
        assert -1.0 <= result <= 1.0


class TestComputePairSimilarity:
    def test_basic(self):
        n = 48
        df_a = pd.DataFrame({
            "year": [2000 + i // 12 for i in range(n)],
            "month": [i % 12 + 1 for i in range(n)],
            "water_area": np.sin(np.linspace(0, 4 * np.pi, n)) * 10 + 100,
        })
        df_b = df_a.copy()
        result = compute_pair_similarity(df_a, df_b)
        assert result["n_common"] == n
        assert result["pearson_r"] == pytest.approx(1.0, abs=1e-6)
        assert result["acf_cos_sim"] == pytest.approx(1.0, abs=1e-6)

    def test_no_overlap(self):
        df_a = pd.DataFrame({
            "year": [2000], "month": [1], "water_area": [100.0]
        })
        df_b = pd.DataFrame({
            "year": [2010], "month": [1], "water_area": [200.0]
        })
        result = compute_pair_similarity(df_a, df_b)
        assert result["n_common"] == 0
        assert np.isnan(result["pearson_r"])
