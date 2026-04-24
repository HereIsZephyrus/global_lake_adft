"""Plot helpers for baseline-model selection and fit diagnostics.

Adapter layer: converts lakeanalysis domain types to generic DataFrames,
then delegates to lakeviz.basemodel for rendering.
"""

from __future__ import annotations

import pandas as pd

from lakeviz.basemodel import (
    plot_candidate_scores as _plot_candidate_scores,
    plot_basis_fit as _plot_basis_fit,
    plot_residuals as _plot_residuals,
)
from .basic import BasisFitRecord


def plot_candidate_scores(records: tuple[BasisFitRecord, ...], criterion: str, selected_basis_name: str):
    scores_df = pd.DataFrame([
        {
            "basis_name": item.basis_name,
            "aic": float(item.aic),
            "bic": float(item.bic),
            "rmse": float(item.rmse),
        }
        for item in records
    ])
    return _plot_candidate_scores(scores_df, criterion, selected_basis_name)


def plot_basis_fit(fit_frame, selected_basis_name, criterion, relative_rmse):
    return _plot_basis_fit(fit_frame, selected_basis_name, criterion, relative_rmse)


def plot_residuals(fit_frame):
    return _plot_residuals(fit_frame)
