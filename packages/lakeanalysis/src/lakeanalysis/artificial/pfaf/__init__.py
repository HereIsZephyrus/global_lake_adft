"""Pfafstetter basin pairing and nearest-natural-lake search."""

from .lookup import (
    fetch_lake_centroids,
    fetch_lake_centroids_chunk,
    lookup_pfaf_chunk,
    lookup_pfaf_ids,
)
from .nearest import compute_nearest_naturals
from .nearest_runner import NearestRunConfig, run_nearest
from .runner import PfafRunConfig, run_pfaf
from .store import (
    ensure_af_nearest_table,
    ensure_lake_pfaf_table,
    upsert_af_nearest,
    upsert_lake_pfaf,
)

__all__ = [
    "fetch_lake_centroids",
    "fetch_lake_centroids_chunk",
    "lookup_pfaf_chunk",
    "lookup_pfaf_ids",
    "compute_nearest_naturals",
    "NearestRunConfig",
    "PfafRunConfig",
    "run_nearest",
    "run_pfaf",
    "ensure_af_nearest_table",
    "ensure_lake_pfaf_table",
    "upsert_af_nearest",
    "upsert_lake_pfaf",
]
