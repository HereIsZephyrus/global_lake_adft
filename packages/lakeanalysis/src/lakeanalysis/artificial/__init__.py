"""Artificial-lake analysis: pfaf pairing, similarity, and human-impact assessment."""

from .fetch import load_pairs_and_areas
from .impact import (
    compute_cv,
    compute_event_stats,
    compute_lake_metrics,
    compute_pair_events,
    compute_pair_metrics,
    compute_pct_change_std,
    compute_range_ratio,
    detect_zscore_events,
)
from .pfaf import (
    compute_nearest_naturals,
    ensure_af_nearest_table,
    ensure_lake_pfaf_table,
    fetch_lake_centroids,
    fetch_lake_centroids_chunk,
    lookup_pfaf_chunk,
    lookup_pfaf_ids,
    upsert_af_nearest,
    upsert_lake_pfaf,
)
from .similarity import (
    acf_cosine_similarity,
    align_series,
    compute_pair_similarity,
    pearson_correlation,
)

__all__ = [
    "acf_cosine_similarity",
    "align_series",
    "compute_cv",
    "compute_event_stats",
    "compute_lake_metrics",
    "compute_nearest_naturals",
    "compute_pair_events",
    "compute_pair_metrics",
    "compute_pair_similarity",
    "compute_pct_change_std",
    "compute_range_ratio",
    "detect_zscore_events",
    "ensure_af_nearest_table",
    "ensure_lake_pfaf_table",
    "fetch_lake_centroids",
    "fetch_lake_centroids_chunk",
    "load_pairs_and_areas",
    "lookup_pfaf_chunk",
    "lookup_pfaf_ids",
    "pearson_correlation",
    "upsert_af_nearest",
    "upsert_lake_pfaf",
]
