"""Backward-compatible figure wrappers for artificial lake impact plots."""

from __future__ import annotations

from lakeviz.domain.artificial import (
    plot_anomaly_ratio_comparison,
    plot_delta_cv_distribution,
    plot_typical_pair_timeline,
    plot_volatility_comparison,
)

__all__ = [
    "plot_anomaly_ratio_comparison",
    "plot_delta_cv_distribution",
    "plot_typical_pair_timeline",
    "plot_volatility_comparison",
]
