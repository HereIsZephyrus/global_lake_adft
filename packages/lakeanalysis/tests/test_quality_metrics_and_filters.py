import numpy as np
import pandas as pd
import pytest

from lakeanalysis.quality.metrics import (
    AgreementConfig,
    classify_agreement,
    classify_outside_range,
    compute_area_range,
    compute_area_ratio,
    compute_flatness_metrics,
    compute_log2_ratio,
    compute_mean_area,
    compute_median_area,
    compute_penalized_volatility,
    compute_quantile_area,
    compute_relative_diff,
    is_anomalous,
)
from lakeanalysis.quality.comparison import (
    _percentile_stats,
    enrich_comparison_df,
    summarize_comparison,
)
from lakeanalysis.quality.classify import (
    classify_area_anomaly,
    default_filters,
)
from lakeanalysis.quality.filters import (
    AnomalyFlag,
    LakeContext,
    decode_anomaly_flags,
    encode_anomaly_flags,
    FLAG_FLAT,
    FLAG_AREA_RATIO,
)
from lakeanalysis.quality.filters.area_ratio import AreaRatioConfig, AreaRatioFilter
from lakeanalysis.quality.filters.flatness import FlatnessFilter, FlatnessFilterConfig
from lakeanalysis.quality.filters.penalized_volatility import (
    PenalizedVolatilityConfig,
    PenalizedVolatilityFilter,
)


# ─── quality/metrics.py ─────────────────────────────────────────────────────


class TestComputeMedianArea:
    def test_basic(self):
        df = pd.DataFrame({"water_area": [10.0, 20.0, 30.0]})
        assert compute_median_area(df) == pytest.approx(20.0)

    def test_even_count(self):
        df = pd.DataFrame({"water_area": [10.0, 20.0, 30.0, 40.0]})
        assert compute_median_area(df) == pytest.approx(25.0)


class TestComputeMeanArea:
    def test_basic(self):
        df = pd.DataFrame({"water_area": [10.0, 20.0, 30.0]})
        assert compute_mean_area(df) == pytest.approx(20.0)


class TestComputeQuantileArea:
    def test_default_75(self):
        df = pd.DataFrame({"water_area": list(range(1, 101))})
        assert compute_quantile_area(df) == pytest.approx(75.25)

    def test_custom_quantile(self):
        df = pd.DataFrame({"water_area": list(range(1, 101))})
        assert compute_quantile_area(df, quantile=0.5) == pytest.approx(50.5)


class TestIsAnomalous:
    def test_zero_is_anomalous(self):
        assert is_anomalous(0.0) is True

    def test_nonzero_not_anomalous(self):
        assert is_anomalous(10.0) is False

    def test_negative_not_anomalous(self):
        assert is_anomalous(-1.0) is False


class TestComputeFlatnessMetrics:
    def test_all_same(self):
        df = pd.DataFrame({"water_area": [5.0] * 10})
        m = compute_flatness_metrics(df)
        assert m["n_obs"] == 10
        assert m["n_unique"] == 1
        assert m["dominant_ratio"] == pytest.approx(1.0)
        assert m["unique_ratio"] == pytest.approx(0.1)

    def test_all_different(self):
        df = pd.DataFrame({"water_area": list(range(10))})
        m = compute_flatness_metrics(df)
        assert m["n_unique"] == 10
        assert m["unique_ratio"] == pytest.approx(1.0)
        assert m["dominant_ratio"] == pytest.approx(0.1)

    def test_empty(self):
        df = pd.DataFrame({"water_area": pd.Series([], dtype=float)})
        m = compute_flatness_metrics(df)
        assert m["n_obs"] == 0
        assert m["dominant_ratio"] == 0.0

    def test_with_rounding(self):
        df = pd.DataFrame({"water_area": [1.001, 1.002, 1.003, 2.0]})
        m = compute_flatness_metrics(df, round_digits=2)
        assert m["n_unique"] == 2

    def test_with_rounding_collapses(self):
        df = pd.DataFrame({"water_area": [1.001, 1.002, 1.003, 2.0]})
        m = compute_flatness_metrics(df, round_digits=0)
        assert m["n_unique"] == 2
        assert m["dominant_ratio"] == pytest.approx(3 / 4)


class TestComputePenalizedVolatility:
    def test_constant_series(self):
        values = pd.Series([5.0] * 20)
        m = compute_penalized_volatility(values)
        assert m["n_obs"] == 20
        assert m["n_distinct"] == 1
        assert m["cv"] == pytest.approx(0.0)
        assert m["H"] == pytest.approx(0.0)
        assert m["h_cv"] == pytest.approx(0.0)

    def test_two_values(self):
        values = pd.Series([10.0] * 5 + [20.0] * 5)
        m = compute_penalized_volatility(values)
        assert m["n_distinct"] == 2
        assert m["H"] == pytest.approx(1.0)
        assert m["cv"] > 0
        assert m["h_cv"] == pytest.approx(m["H"] * m["cv"])

    def test_single_value(self):
        values = pd.Series([5.0])
        m = compute_penalized_volatility(values)
        assert m["cv"] is None
        assert m["penalized_volatility"] is None

    def test_empty(self):
        values = pd.Series([], dtype=float)
        m = compute_penalized_volatility(values)
        assert m["n_obs"] == 0
        assert m["penalized_volatility"] is None

    def test_zero_mean(self):
        values = pd.Series([0.0] * 10)
        m = compute_penalized_volatility(values)
        assert m["cv"] is None
        assert m["h_cv"] is None


class TestComputeAreaRange:
    def test_basic(self):
        df = pd.DataFrame({"water_area": [5.0, 10.0, 15.0]})
        r = compute_area_range(df)
        assert r["min_area"] == 5.0
        assert r["max_area"] == 15.0

    def test_empty(self):
        df = pd.DataFrame({"water_area": pd.Series([], dtype=float)})
        r = compute_area_range(df)
        assert r["min_area"] == 0.0
        assert r["max_area"] == 0.0


class TestClassifyOutsideRange:
    def test_within_range(self):
        r = classify_outside_range(10.0, 5.0, 15.0)
        assert r["is_outside_range"] is False

    def test_below_min(self):
        r = classify_outside_range(3.0, 5.0, 15.0)
        assert r["is_outside_range"] is True
        assert r["is_below_min"] is True

    def test_above_max(self):
        r = classify_outside_range(20.0, 5.0, 15.0)
        assert r["is_outside_range"] is True
        assert r["is_above_max"] is True

    def test_atlas_zero(self):
        r = classify_outside_range(0.0, 5.0, 15.0)
        assert r["is_outside_range"] is False

    def test_atlas_negative(self):
        r = classify_outside_range(-1.0, 5.0, 15.0)
        assert r["is_outside_range"] is False


class TestComputeAreaRatio:
    def test_basic(self):
        result = compute_area_ratio(np.array([10.0]), np.array([5.0]))
        assert result[0] == pytest.approx(2.0)

    def test_atlas_zero(self):
        result = compute_area_ratio(np.array([10.0]), np.array([0.0]))
        assert np.isnan(result[0])

    def test_array(self):
        result = compute_area_ratio(
            np.array([10.0, 20.0]), np.array([5.0, 10.0])
        )
        np.testing.assert_allclose(result, [2.0, 2.0])


class TestComputeRelativeDiff:
    def test_basic(self):
        result = compute_relative_diff(np.array([10.0]), np.array([5.0]))
        assert result[0] == pytest.approx(1.0)

    def test_equal(self):
        result = compute_relative_diff(np.array([5.0]), np.array([5.0]))
        assert result[0] == pytest.approx(0.0)


class TestComputeLog2Ratio:
    def test_double(self):
        result = compute_log2_ratio(np.array([10.0]), np.array([5.0]))
        assert result[0] == pytest.approx(1.0)

    def test_equal(self):
        result = compute_log2_ratio(np.array([5.0]), np.array([5.0]))
        assert result[0] == pytest.approx(0.0)

    def test_zero_rs(self):
        result = compute_log2_ratio(np.array([0.0]), np.array([5.0]))
        assert np.isnan(result[0])


class TestClassifyAgreement:
    def test_good(self):
        result = classify_agreement(np.array([1.0]))
        assert result[0] == "good"

    def test_moderate(self):
        result = classify_agreement(np.array([3.0]))
        assert result[0] == "moderate"

    def test_poor(self):
        result = classify_agreement(np.array([15.0]))
        assert result[0] == "poor"

    def test_nan_is_poor(self):
        result = classify_agreement(np.array([np.nan]))
        assert result[0] == "poor"

    def test_custom_config(self):
        cfg = AgreementConfig(good=1.5, moderate=3.0, poor=5.0)
        result = classify_agreement(np.array([1.8]), cfg)
        assert result[0] == "moderate"


# ─── quality/comparison.py ──────────────────────────────────────────────────


class TestPercentileStats:
    def test_basic(self):
        ratio = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        log2 = np.log2(ratio)
        stats = _percentile_stats(ratio, log2)
        assert stats["median_ratio"] == pytest.approx(3.0)
        assert stats["p50_ratio"] == pytest.approx(3.0)
        assert "iqr_ratio" in stats
        assert "mean_log2_ratio" in stats
        assert "std_log2_ratio" in stats

    def test_empty_ratio(self):
        stats = _percentile_stats(np.array([]), np.array([]))
        assert np.isnan(stats["median_ratio"])
        assert np.isnan(stats["iqr_ratio"])

    def test_single_value(self):
        stats = _percentile_stats(np.array([2.0]), np.array([1.0]))
        assert stats["median_ratio"] == pytest.approx(2.0)
        assert np.isnan(stats["iqr_ratio"])
        assert np.isnan(stats["std_log2_ratio"])


class TestSummarizeComparison:
    def test_basic(self):
        df = pd.DataFrame({
            "rs_area_median": [10.0, 20.0, 30.0],
            "atlas_area": [10.0, 10.0, 10.0],
        })
        result = summarize_comparison(df)
        assert result["n_total"] == 3
        assert "median_ratio" in result
        assert "n_overestimate" in result

    def test_empty_after_filter(self):
        df = pd.DataFrame({
            "rs_area_median": [10.0],
            "atlas_area": [0.0],
        })
        result = summarize_comparison(df)
        assert result["n_total"] == 0

    def test_all_nan(self):
        df = pd.DataFrame({
            "rs_area_median": [np.nan],
            "atlas_area": [np.nan],
        })
        result = summarize_comparison(df)
        assert result["n_total"] == 0


class TestEnrichComparisonDf:
    def test_adds_columns(self):
        df = pd.DataFrame({
            "rs_area_mean": [10.0, 20.0],
            "rs_area_median": [10.0, 20.0],
            "atlas_area": [10.0, 10.0],
        })
        result = enrich_comparison_df(df)
        assert "ratio_mean" in result.columns
        assert "ratio_median" in result.columns
        assert "rel_diff_mean" in result.columns
        assert "log2_ratio_mean" in result.columns
        assert "agreement_mean" in result.columns


# ─── quality/filters ────────────────────────────────────────────────────────


def _make_ctx(
    areas: list[float],
    atlas_area: float = 100.0,
) -> LakeContext:
    n = len(areas)
    years = [2000 + i // 12 for i in range(n)]
    months = [i % 12 + 1 for i in range(n)]
    df = pd.DataFrame({"water_area": areas, "year": years, "month": months})
    median = float(np.median(areas)) if areas else 0.0
    mean = float(np.mean(areas)) if areas else 0.0
    return LakeContext(
        df=df,
        df_no_frozen=df,
        rs_area_median=median,
        rs_area_mean=mean,
        rs_area_quantile=float(np.percentile(areas, 75)) if areas else 0.0,
        atlas_area=atlas_area,
    )


class TestAreaRatioFilter:
    def test_within_range(self):
        ctx = _make_ctx([100.0] * 10, atlas_area=100.0)
        f = AreaRatioFilter()
        flag = f.classify(ctx)
        assert not flag.is_anomaly
        assert flag.detail["area_ratio"] == pytest.approx(1.0)

    def test_too_high(self):
        ctx = _make_ctx([1000.0] * 10, atlas_area=1.0)
        f = AreaRatioFilter()
        flag = f.classify(ctx)
        assert flag.is_anomaly

    def test_too_low(self):
        ctx = _make_ctx([1.0] * 10, atlas_area=1000.0)
        f = AreaRatioFilter()
        flag = f.classify(ctx)
        assert flag.is_anomaly

    def test_custom_config(self):
        ctx = _make_ctx([50.0] * 10, atlas_area=100.0)
        cfg = AreaRatioConfig(min_ratio=0.6, max_ratio=1.5)
        f = AreaRatioFilter(cfg)
        flag = f.classify(ctx)
        assert flag.is_anomaly


class TestFlatnessFilter:
    def test_flat_series(self):
        ctx = _make_ctx([5.0] * 20)
        f = FlatnessFilter()
        flag = f.classify(ctx)
        assert flag.is_anomaly
        assert flag.detail["dominant_ratio"] == pytest.approx(1.0)

    def test_varied_series(self):
        ctx = _make_ctx(list(range(1, 21)))
        f = FlatnessFilter()
        flag = f.classify(ctx)
        assert not flag.is_anomaly

    def test_custom_threshold(self):
        ctx = _make_ctx([5.0] * 6 + [10.0] * 4)
        cfg = FlatnessFilterConfig(dominant_ratio_threshold=0.5)
        f = FlatnessFilter(cfg)
        flag = f.classify(ctx)
        assert flag.is_anomaly


class TestPenalizedVolatilityFilter:
    def test_constant_series_flagged(self):
        ctx = _make_ctx([5.0] * 20)
        f = PenalizedVolatilityFilter()
        flag = f.classify(ctx)
        assert flag.is_anomaly

    def test_varied_series_not_flagged(self):
        ctx = _make_ctx([float(x) for x in range(1, 21)])
        f = PenalizedVolatilityFilter()
        flag = f.classify(ctx)
        assert not flag.is_anomaly

    def test_custom_threshold(self):
        ctx = _make_ctx([10.0] * 10 + [11.0] * 10)
        cfg = PenalizedVolatilityConfig(pv_threshold=10.0)
        f = PenalizedVolatilityFilter(cfg)
        flag = f.classify(ctx)
        assert flag.is_anomaly


# ─── quality/classify.py ────────────────────────────────────────────────────


class TestClassifyAreaAnomaly:
    def test_no_anomaly_subset_filters(self):
        """Test with a subset of filters that don't require unit alignment."""
        from lakeanalysis.quality.filters.flatness import FlatnessFilter
        from lakeanalysis.quality.filters.area_ratio import AreaRatioFilter

        ctx = _make_ctx([float(x) for x in range(50, 150)], atlas_area=100.0)
        filters = [FlatnessFilter(), AreaRatioFilter()]
        result = classify_area_anomaly(ctx, filters)
        assert result["is_anomalous"] is False

    def test_flat_anomaly(self):
        ctx = _make_ctx([5e6] * 50, atlas_area=5.0)
        filters = default_filters()
        result = classify_area_anomaly(ctx, filters)
        assert result["is_anomalous"] is True
        assert result["is_flat"] is True
        assert result["anomaly_flags"] & FLAG_FLAT

    def test_area_ratio_anomaly(self):
        ctx = _make_ctx([1000e6] * 50, atlas_area=1.0)
        filters = default_filters()
        result = classify_area_anomaly(ctx, filters)
        assert result["is_anomalous"] is True
        assert result["is_area_ratio"] is True
        assert result["anomaly_flags"] & FLAG_AREA_RATIO


class TestDecodeEncodeFlags:
    def test_roundtrip(self):
        flags = FLAG_FLAT | FLAG_AREA_RATIO
        decoded = decode_anomaly_flags(flags)
        assert decoded["flat"] is True
        assert decoded["area_ratio"] is True
        assert decoded["zero_quantile"] is False
        encoded = encode_anomaly_flags(decoded)
        assert encoded == flags

    def test_zero(self):
        decoded = decode_anomaly_flags(0)
        assert all(v is False for v in decoded.values())
        assert encode_anomaly_flags(decoded) == 0
