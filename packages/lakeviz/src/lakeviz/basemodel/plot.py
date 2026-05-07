"""Baseline-model selection and fit diagnostics plots.

All functions accept only plain Python types and pandas DataFrames —
no lakeanalysis domain types are required.
"""

from __future__ import annotations

from lakeviz.style.presets import Theme
from lakeviz.domain.basemodel import (
    plot_basis_fit as _plot_basis_fit,
    plot_candidate_scores as _plot_candidate_scores,
    plot_residuals as _plot_residuals,
)


def plot_candidate_scores(scores_df, criterion, selected_basis_name):
    Theme.apply()
    return _plot_candidate_scores(scores_df, criterion, selected_basis_name)


def plot_basis_fit(fit_frame, selected_basis_name, criterion, relative_rmse):
    Theme.apply()
    return _plot_basis_fit(fit_frame, selected_basis_name, criterion, relative_rmse)


def plot_residuals(fit_frame):
    Theme.apply()
    return _plot_residuals(fit_frame)


__all__ = [
    "plot_candidate_scores",
    "plot_basis_fit",
    "plot_residuals",
]
