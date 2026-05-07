"""Backward-compatible figure wrappers for Hawkes diagnostics."""

from __future__ import annotations

from lakeviz.style.presets import Theme
from lakeviz.domain.hawkes import (
    plot_event_timeline as _plot_event_timeline,
    plot_intensity_decomposition as _plot_intensity_decomposition,
    plot_kernel_matrix as _plot_kernel_matrix,
    plot_lrt_summary as _plot_lrt_summary,
)


def plot_event_timeline(events_table):
    Theme.apply()
    return _plot_event_timeline(events_table)


def plot_intensity_decomposition(decomposition_df):
    Theme.apply()
    return _plot_intensity_decomposition(decomposition_df)


def plot_kernel_matrix(alpha, beta):
    Theme.apply()
    return _plot_kernel_matrix(alpha, beta)


def plot_lrt_summary(lrt_df):
    Theme.apply()
    return _plot_lrt_summary(lrt_df)

__all__ = [
    "plot_event_timeline",
    "plot_intensity_decomposition",
    "plot_kernel_matrix",
    "plot_lrt_summary",
]
