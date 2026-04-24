"""Lake-pair similarity: Pearson correlation and ACF cosine (12-month delay)."""

from .compute import (
    acf_cosine_similarity,
    compute_pair_similarity,
    pearson_correlation,
)
from .fetch import load_pairs_and_areas
from lakeviz.similarity import (
    plot_acf_cosine_distribution,
    plot_pearson_distribution,
    plot_pearson_vs_acf,
)

__all__ = [
    "acf_cosine_similarity",
    "compute_pair_similarity",
    "pearson_correlation",
    "load_pairs_and_areas",
    "plot_pearson_distribution",
    "plot_acf_cosine_distribution",
    "plot_pearson_vs_acf",
]
