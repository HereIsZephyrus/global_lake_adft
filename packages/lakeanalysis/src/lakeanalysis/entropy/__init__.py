"""Apportionment Entropy (AE) computation and visualisation package."""

from .compute import (
    ae_from_values,
    compute_annual_ae,
    compute_overall_ae,
    compute_trend,
)
from .runner import (
    EntropyRunConfig,
    load_entropy_summary,
    run_entropy,
    run_update_amplitude_only,
    show_entropy_plots,
    write_amplitude_entropy_csv,
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
    "EntropyRunConfig",
    "load_entropy_summary",
    "plot_ae_distribution",
    "plot_annual_ae",
    "plot_trend_summary",
    "remove_amplitude_outliers",
    "run_entropy",
    "run_update_amplitude_only",
    "show_entropy_plots",
    "write_amplitude_entropy_csv",
]
