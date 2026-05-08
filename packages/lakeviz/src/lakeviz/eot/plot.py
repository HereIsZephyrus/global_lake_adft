"""Backward-compatible figure wrappers for EOT diagnostics plots."""

from __future__ import annotations

from lakeviz.domain.eot import (
    plot_eot_extremes,
    plot_extremes_timeline,
    plot_extremes_with_hawkes,
    plot_location_model,
    plot_mrl,
    plot_parameter_stability,
    plot_pp,
    plot_qq,
    plot_return_levels,
)


__all__ = [
    "plot_mrl",
    "plot_parameter_stability",
    "plot_extremes_timeline",
    "plot_pp",
    "plot_qq",
    "plot_return_levels",
    "plot_location_model",
    "plot_eot_extremes",
    "plot_extremes_with_hawkes",
]
