"""Lake-pair similarity: Pearson correlation and ACF cosine (12-month delay)."""

from .compute import (
    acf_cosine_similarity,
    align_series,
    compute_pair_similarity,
    pearson_correlation,
)
from .runner import (
    SimilarityRunConfig,
    load_similarity_summary,
    run_similarity,
    show_similarity_plots,
)

__all__ = [
    "acf_cosine_similarity",
    "align_series",
    "compute_pair_similarity",
    "SimilarityRunConfig",
    "load_similarity_summary",
    "pearson_correlation",
    "run_similarity",
    "show_similarity_plots",
]
