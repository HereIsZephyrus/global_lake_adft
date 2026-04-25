"""Periodic baseline models and model selection utilities."""

from .basic import BaseBasis, BasisFitRecord
from .harmonic import HarmonicBasis
from .plot_adapter import (
    plot_basis_fit,
    plot_candidate_scores,
    plot_residuals,
)
from .selector import BasisSelectionResult, BasisSelector

__all__ = [
    "BaseBasis",
    "BasisFitRecord",
    "HarmonicBasis",
    "BasisSelectionResult",
    "BasisSelector",
    "plot_candidate_scores",
    "plot_basis_fit",
    "plot_residuals",
]
