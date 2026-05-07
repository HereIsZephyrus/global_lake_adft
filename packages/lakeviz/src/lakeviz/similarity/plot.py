"""Backward-compatible figure wrappers for lake-pair similarity plots."""

from __future__ import annotations

from lakeviz.domain.similarity import (
    plot_acf_cosine_distribution,
    plot_pearson_distribution,
    plot_pearson_vs_acf,
)

__all__ = [
    "plot_pearson_distribution",
    "plot_acf_cosine_distribution",
    "plot_pearson_vs_acf",
]
