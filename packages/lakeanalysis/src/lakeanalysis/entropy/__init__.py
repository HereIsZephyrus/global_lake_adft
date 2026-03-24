"""Apportionment Entropy (AE) computation and visualisation package."""

from .compute import (
    ae_from_values,
    compute_annual_ae,
    compute_overall_ae,
    compute_trend,
)
from .plot import (
    plot_ae_distribution,
    plot_annual_ae,
    plot_trend_summary,
)

__all__ = [
    "ae_from_values",
    "compute_annual_ae",
    "compute_overall_ae",
    "compute_trend",
    "plot_ae_distribution",
    "plot_annual_ae",
    "plot_trend_summary",
]
