import matplotlib
matplotlib.use("Agg")

import numpy as np
import pandas as pd
import pytest

from lakeanalysis.eot.basis import BasisFitRecord, BasisSelector
from lakeanalysis.eot.diagnostics import ModelChecker, ReturnLevelEstimator
from lakeanalysis.eot.estimation import EOTEstimator
from lakeanalysis.eot.models import FitResult, LocationModel
from lakeanalysis.eot.plot_adapter import (
    plot_candidate_scores,
    plot_basis_fit,
    plot_residuals,
    plot_eot_extremes,
    plot_extremes_timeline,
    plot_location_model,
    plot_mrl,
    plot_parameter_stability,
    plot_pp,
    plot_qq,
    plot_return_levels,
)
from lakeanalysis.eot.preprocess import ThresholdSelector
from lakeanalysis.eot.series import MonthlyTimeSeries


def _make_series(years=3):
    rows = []
    for year in range(2000, 2000 + years):
        for month in range(1, 13):
            val = 100.0 + 10.0 * np.sin(2 * np.pi * month / 12) + year * 2.0
            rows.append({"year": year, "month": month, "water_area": val})
    return MonthlyTimeSeries.from_frame(pd.DataFrame(rows))


def _make_fit_result(n_extremes=5):
    series = _make_series(years=5)
    lm = LocationModel(n_harmonics=1, include_trend=True)
    theta = np.array([105.0, 2.0, 5.0, 3.0, 8.0, 0.05])
    extremes = pd.DataFrame({
        "year": [2000] * n_extremes,
        "month": [6 + i for i in range(n_extremes)],
        "time": np.linspace(0.4, 4.0, n_extremes),
        "value": np.linspace(110, 120, n_extremes),
        "original_value": np.linspace(110, 120, n_extremes),
        "cluster_id": list(range(1, n_extremes + 1)),
        "cluster_size": [1] * n_extremes,
        "threshold": np.full(n_extremes, 100.0, dtype=float),
    })
    return FitResult(
        theta=theta,
        covariance=np.diag([0.1] * len(theta)),
        threshold=100.0,
        tail="high",
        log_likelihood=-50.0,
        converged=True,
        message="ok",
        location_model=lm,
        series=series,
        extremes=extremes,
    )


class TestPlotMRL:
    def test_plot_mrl_does_not_raise(self):
        estimator = EOTEstimator()
        series = _make_series()
        diags = estimator.threshold_diagnostics(series)
        fig = plot_mrl(diags["mrl"])
        assert fig is not None


class TestPlotParameterStability:
    def test_plot_stability_does_not_raise(self):
        estimator = EOTEstimator()
        series = _make_series()
        diags = estimator.threshold_diagnostics(series)
        fig = plot_parameter_stability(diags["stability"])
        assert fig is not None


class TestPlotExtremesTimeline:
    def test_plot_extremes_timeline_does_not_raise(self):
        fr = _make_fit_result()
        fig = plot_extremes_timeline(fr.series, fr.extremes, 100.0)
        assert fig is not None

    def test_plot_extremes_timeline_with_fit_result(self):
        fr = _make_fit_result()
        fig = plot_extremes_timeline(fr.series, fr.extremes, 100.0, fit_result=fr)
        assert fig is not None


class TestPlotPPQQ:
    def test_plot_pp_does_not_raise(self):
        fr = _make_fit_result()
        checker = ModelChecker(fr)
        fig = plot_pp(checker)
        assert fig is not None

    def test_plot_qq_does_not_raise(self):
        fr = _make_fit_result()
        checker = ModelChecker(fr)
        fig = plot_qq(checker)
        assert fig is not None


class TestPlotReturnLevels:
    def test_plot_return_levels_dataframe(self):
        fr = _make_fit_result()
        rle = ReturnLevelEstimator(fr)
        df = rle.estimate()
        fig = plot_return_levels(df)
        assert fig is not None

    def test_plot_return_levels_estimator(self):
        fr = _make_fit_result()
        rle = ReturnLevelEstimator(fr)
        fig = plot_return_levels(rle)
        assert fig is not None


class TestPlotLocationModel:
    def test_plot_location_model_does_not_raise(self):
        fr = _make_fit_result()
        fig = plot_location_model(fr)
        assert fig is not None


class TestPlotEOTExtremes:
    def test_plot_eot_extremes_does_not_raise(self):
        series = _make_series(years=1)
        n = series.n_obs
        extremes = pd.DataFrame({
            "year": series.data["year"].tolist(),
            "month": series.data["month"].tolist(),
            "water_area": series.data["original_value"].tolist(),
            "time": np.linspace(0, 1, n),
            "value": series.values,
            "original_value": series.original_values,
            "tail": ["high"] * n,
            "threshold_at_event": [105.0] * n,
        })
        series_df = series.data[["year", "month", "time", "original_value"]].copy()
        series_df["water_area"] = series.data["original_value"]
        fig = plot_eot_extremes(
            hylak_id=12345,
            series_df=series_df,
            extremes_df=extremes,
        )
        assert fig is not None


class TestBasisPlots:
    def test_plot_candidate_scores_does_not_raise(self):
        selector = BasisSelector(criterion="aic")
        series = _make_series(years=5)
        times = series.data["time"].to_numpy(dtype=float)
        result = selector.select_result(times, series.values)
        fig = plot_candidate_scores(
            result.candidate_records,
            criterion="aic",
            selected_basis_name=result.selected_basis.model_name,
        )
        assert fig is not None

    def test_plot_residuals_does_not_raise(self):
        selector = BasisSelector()
        series = _make_series(years=5)
        times = series.data["time"].to_numpy(dtype=float)
        basis = selector.select(times, series.values)
        _, fitted, residuals = selector.fit_basis(times, series.values, basis)
        fit_frame = pd.DataFrame({
            "time": times,
            "value": series.values,
            "fitted": fitted,
            "residual": residuals,
        })
        fig = plot_residuals(fit_frame)
        assert fig is not None
