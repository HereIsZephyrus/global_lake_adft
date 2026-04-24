"""Plot helpers for Hawkes fit diagnostics.

Adapter layer: converts lakeanalysis domain types to generic DataFrames/arrays,
then delegates to lakeviz.hawkes for rendering.
"""

from __future__ import annotations

import pandas as pd

from lakeviz.hawkes import (
    plot_event_timeline as _plot_event_timeline,
    plot_intensity_decomposition as _plot_intensity_decomposition,
    plot_kernel_matrix as _plot_kernel_matrix,
    plot_lrt_summary as _plot_lrt_summary,
)
from .types import HawkesFitResult, LRTResult


def plot_event_timeline(events_table):
    return _plot_event_timeline(events_table)


def plot_intensity_decomposition(decomposition_df):
    return _plot_intensity_decomposition(decomposition_df)


def plot_kernel_matrix(fit_result: HawkesFitResult):
    return _plot_kernel_matrix(alpha=fit_result.alpha, beta=fit_result.beta)


def plot_lrt_summary(lrt_results):
    if isinstance(lrt_results, list):
        frame = pd.DataFrame([
            {
                "test_name": item.test_name,
                "lr_statistic": item.lr_statistic,
                "p_value": item.p_value,
                "significance_level": item.significance_level,
            }
            for item in lrt_results
        ])
    else:
        frame = lrt_results
    return _plot_lrt_summary(frame)
