import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.basis import HarmonicBasis
from lakeanalysis.eot.preprocess import (
    NoDeclustering,
    QuantileThresholdModel,
    RunsDeclustering,
    ThresholdSelector,
    _broadcast_threshold,
)
from lakeanalysis.eot.series import MonthlyTimeSeries


def _make_series(years=3):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            val = 100.0 + 10.0 * np.sin(2 * np.pi * month / 12) + year * 2.0
            rows.append({"year": year, "month": month, "water_area": val})
    return MonthlyTimeSeries.from_frame(pd.DataFrame(rows))


class TestQuantileThresholdModel:
    def test_default_construction(self):
        qtm = QuantileThresholdModel()
        assert qtm.include_trend is True
        assert qtm.n_harmonics == 1
        assert isinstance(qtm.basis_model, HarmonicBasis)

    def test_custom_basis(self):
        basis = HarmonicBasis(n_harmonics=2)
        qtm = QuantileThresholdModel(basis_model=basis)
        assert qtm.basis_model is basis

    def test_design_matrix_shape(self):
        qtm = QuantileThresholdModel(n_harmonics=1, include_trend=True)
        times = np.linspace(0, 3, 36)
        dm = qtm.design_matrix(times)
        assert dm.shape == (36, 4)

    def test_fit_returns_params_on_seasonal_data(self):
        qtm = QuantileThresholdModel(n_harmonics=1, include_trend=True)
        series = _make_series()
        times = series.data["time"].to_numpy(dtype=float)
        values = series.values
        params = qtm.fit(times, values, quantile=0.90)
        assert params.ndim == 1
        assert len(params) == 4

    def test_fit_quantile_out_of_range_raises(self):
        qtm = QuantileThresholdModel()
        times = np.array([0.0, 1.0, 2.0])
        values = np.array([1.0, 2.0, 3.0])
        with pytest.raises(ValueError, match="quantile must be in \\(0, 1\\)"):
            qtm.fit(times, values, quantile=1.5)
        with pytest.raises(ValueError, match="quantile must be in \\(0, 1\\)"):
            qtm.fit(times, values, quantile=0.0)

    def test_evaluate(self):
        qtm = QuantileThresholdModel(n_harmonics=1, include_trend=False)
        params = np.array([10.0, 2.0, 3.0])
        times = np.linspace(0, 2, 5)
        result = qtm.evaluate(times, params)
        assert result.shape == (5,)
        assert np.all(np.isfinite(result))


class TestBroadcastThreshold:
    def test_scalar_broadcasts(self):
        result = _broadcast_threshold(5.0, 3)
        np.testing.assert_array_equal(result, np.array([5.0, 5.0, 5.0]))

    def test_array_passes_through(self):
        arr = np.array([1.0, 2.0, 3.0])
        result = _broadcast_threshold(arr, 3)
        np.testing.assert_array_equal(result, arr)

    def test_length_mismatch_raises(self):
        with pytest.raises(ValueError, match="threshold array length"):
            _broadcast_threshold(np.array([1.0, 2.0]), 3)


class TestNoDeclustering:
    def test_basic_decluster(self):
        series = _make_series(years=1)
        decluster = NoDeclustering()
        result = decluster.decluster(series, threshold=105.0)
        assert not result.empty
        assert "cluster_id" in result.columns
        assert "cluster_size" in result.columns
        assert (result["cluster_size"] == 1).all()

    def test_empty_if_no_exceedances(self):
        series = _make_series(years=1)
        decluster = NoDeclustering()
        result = decluster.decluster(series, threshold=float(series.values.max()) + 1000.0)
        assert result.empty

    def test_all_exceedances_kept(self):
        series = _make_series(years=1)
        decluster = NoDeclustering()
        result = decluster.decluster(series, threshold=0.0)
        assert len(result) == series.n_obs

    def test_cluster_id_starts_at_one(self):
        series = _make_series(years=1)
        decluster = NoDeclustering()
        result = decluster.decluster(series, threshold=105.0)
        assert result["cluster_id"].min() == 1


class TestRunsDeclustering:
    def test_basic_decluster(self):
        series = _make_series(years=1)
        decluster = RunsDeclustering(run_length=1)
        result = decluster.decluster(series, threshold=105.0)
        assert not result.empty
        assert "cluster_id" in result.columns
        assert "cluster_size" in result.columns

    def test_empty_if_no_exceedances(self):
        series = _make_series(years=1)
        decluster = RunsDeclustering()
        result = decluster.decluster(series, threshold=float(series.values.max()) + 1000.0)
        assert result.empty

    def test_run_length_invalid_raises(self):
        with pytest.raises(ValueError, match="run_length must be >= 1"):
            RunsDeclustering(run_length=0)

    def test_cluster_id_starts_at_one(self):
        series = _make_series(years=1)
        decluster = RunsDeclustering(run_length=2)
        result = decluster.decluster(series, threshold=105.0)
        assert result["cluster_id"].min() == 1

    def test_array_threshold_accepted(self):
        series = _make_series(years=1)
        decluster = RunsDeclustering(run_length=1)
        threshold_array = np.full(series.n_obs, 105.0)
        result = decluster.decluster(series, threshold_array)
        assert "threshold" in result.columns


class TestThresholdSelector:
    def test_default_construction(self):
        ts = ThresholdSelector()
        assert ts.min_exceedances == 8
        assert ts.lower_quantile == 0.70
        assert ts.upper_quantile == 0.98
        assert ts.n_thresholds == 25

    def test_candidate_thresholds_returns_unique(self):
        ts = ThresholdSelector()
        series = _make_series()
        candidates = ts.candidate_thresholds(series)
        assert len(candidates) <= ts.n_thresholds
        assert np.all(np.diff(candidates) > 0)

    def test_mean_residual_life_returns_dataframe(self):
        ts = ThresholdSelector(min_exceedances=3)
        series = _make_series()
        mrl = ts.mean_residual_life(series)
        assert isinstance(mrl, pd.DataFrame)
        assert set(mrl.columns) == {"threshold", "mean_excess", "n_exceedances"}

    def test_parameter_stability_returns_dataframe(self):
        ts = ThresholdSelector(min_exceedances=5)
        series = _make_series()
        stability = ts.parameter_stability(series)
        assert isinstance(stability, pd.DataFrame)
        assert set(stability.columns) == {
            "threshold", "shape_xi", "scale_sigma_u", "modified_scale", "n_exceedances",
        }

    def test_fit_threshold_returns_params_and_u_obs(self):
        ts = ThresholdSelector()
        series = _make_series()
        params, u_obs = ts.fit_threshold(series, quantile=0.90)
        assert params.ndim == 1
        assert len(u_obs) == series.n_obs

    def test_suggest_threshold_returns_median(self):
        ts = ThresholdSelector()
        series = _make_series()
        val = ts.suggest_threshold(series, quantile=0.90)
        assert isinstance(val, float)
        assert np.isfinite(val)

    def test_suggest_threshold_invalid_quantile_raises(self):
        ts = ThresholdSelector()
        series = _make_series()
        with pytest.raises(ValueError, match="quantile must be in \\(0, 1\\)"):
            ts.suggest_threshold(series, quantile=2.0)
