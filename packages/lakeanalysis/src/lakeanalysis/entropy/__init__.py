"""Apportionment Entropy (AE) computation and visualisation package."""

from .compute import (
    ae_from_values,
    compute_annual_ae,
    compute_overall_ae,
    compute_trend,
)
from lakeviz.entropy import (
    plot_ae_distribution,
    plot_annual_ae,
    plot_trend_summary,
    remove_amplitude_outliers,
)

__all__ = [
    "ae_from_values",
    "compute_annual_ae",
    "compute_overall_ae",
    "compute_trend",
    "plot_ae_distribution",
    "plot_annual_ae",
    "plot_trend_summary",
    "remove_amplitude_outliers",
]
