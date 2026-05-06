"""Lake-pair similarity: Pearson correlation and ACF cosine (12-month delay)."""

from .compute import (
    acf_cosine_similarity,
    align_series,
    compute_pair_similarity,
    pearson_correlation,
)

__all__ = [
    "acf_cosine_similarity",
    "align_series",
    "compute_pair_similarity",
    "pearson_correlation",
]
