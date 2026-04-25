"""Plot helpers for EOT threshold diagnostics and NHPP model evaluation.

Adapter layer: converts lakeanalysis domain types to generic DataFrames,
then delegates to lakeviz.eot for rendering.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from lakeviz.eot import (
    plot_mrl as _plot_mrl,
    plot_parameter_stability as _plot_parameter_stability,
    plot_extremes_timeline as _plot_extremes_timeline,
    plot_pp as _plot_pp,
    plot_qq as _plot_qq,
    plot_return_levels as _plot_return_levels,
    plot_location_model as _plot_location_model,
    plot_eot_extremes_from_db as _plot_eot_extremes_from_db,
)
from .diagnostics import ModelChecker, ReturnLevelEstimator
from .estimation import FitResult
from .preprocess import MonthlyTimeSeries


def plot_mrl(mrl_df):
    return _plot_mrl(mrl_df)


def plot_parameter_stability(stability_df):
    return _plot_parameter_stability(stability_df)


def plot_extremes_timeline(
    series: MonthlyTimeSeries,
    extremes,
    threshold: float,
    fit_result: "FitResult | None" = None,
):
    displayed_series = (
        fit_result.full_series
        if fit_result is not None and fit_result.full_series is not None
        else series
    )
    series_df = displayed_series.data[["time", "original_value"]]
    direction = displayed_series.direction

    threshold_curve_df = None
    if fit_result is not None and fit_result.threshold_params is not None:
        times = series_df["time"].to_numpy(dtype=float)
        u_curve = fit_result.threshold_at(times)
        displayed_u = -u_curve if direction == "low" else u_curve
        threshold_curve_df = pd.DataFrame({"time": times, "threshold": displayed_u})

    return _plot_extremes_timeline(
        series_df=series_df,
        extremes=extremes,
        direction=direction,
        threshold=threshold,
        threshold_curve_df=threshold_curve_df,
    )


def plot_pp(checker: ModelChecker):
    return _plot_pp(checker.probability_plot_data())


def plot_qq(checker: ModelChecker):
    return _plot_qq(checker.quantile_plot_data())


def plot_return_levels(return_levels):
    if isinstance(return_levels, ReturnLevelEstimator):
        return_levels = return_levels.estimate()
    return _plot_return_levels(return_levels)


def plot_location_model(fit_result: FitResult, n_points: int = 400):
    reference_series = fit_result.full_series if fit_result.full_series is not None else fit_result.series
    grid = np.linspace(0.0, reference_series.duration_years, n_points)
    mu_values = fit_result.mu(grid)
    mu_df = pd.DataFrame({"time": grid, "mu": mu_values})
    extremes_df = fit_result.extremes[["time", "value"]].copy()

    threshold_curve_df = None
    threshold_val = None
    if fit_result.threshold_params is not None:
        u_curve = fit_result.threshold_at(grid)
        threshold_curve_df = pd.DataFrame({"time": grid, "threshold": u_curve})
    else:
        threshold_val = fit_result.threshold

    return _plot_location_model(
        mu_df=mu_df,
        extremes_df=extremes_df,
        threshold=threshold_val,
        threshold_curve_df=threshold_curve_df,
    )


def plot_eot_extremes_from_db(hylak_id, series_df, extremes_df, annotate_top_n_each_tail=8):
    return _plot_eot_extremes_from_db(
        hylak_id=hylak_id,
        series_df=series_df,
        extremes_df=extremes_df,
        annotate_top_n_each_tail=annotate_top_n_each_tail,
    )
