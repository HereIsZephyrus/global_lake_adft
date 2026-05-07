"""Backward-compatible figure wrappers for Hawkes diagnostics."""

from __future__ import annotations

from lakeviz.domain.hawkes import (
    plot_event_timeline,
    plot_intensity_decomposition,
    plot_kernel_matrix,
    plot_lrt_summary,
)

__all__ = [
    "plot_event_timeline",
    "plot_intensity_decomposition",
    "plot_kernel_matrix",
    "plot_lrt_summary",
]
