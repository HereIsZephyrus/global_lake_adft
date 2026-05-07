"""Application runner for the Pfafstetter lookup pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider

from .lookup import lookup_pfaf_chunk

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class PfafRunConfig:
    limit_id: int | None = None
    chunk_size: int = 10_000


def run_pfaf(config: PfafRunConfig) -> None:
    """Execute the pfaf lookup pipeline in resumable chunks."""
    log.info(
        "Starting pfaf lookup pipeline, limit_id=%s, chunk_size=%d",
        config.limit_id,
        config.chunk_size,
    )

    provider = create_provider(SourceConfig())
    provider.ensure_table("lake_pfaf")
    max_id = provider.fetch_max_hylak_id()
    if config.limit_id is not None:
        max_id = min(max_id, config.limit_id - 1)
    for chunk_start in range(0, max_id + 1, config.chunk_size):
        chunk_end = min(chunk_start + config.chunk_size, max_id + 1)
        done_ids = provider.fetch_done_ids("lake_pfaf", chunk_start, chunk_end)
        centroids = [
            item for item in provider.fetch_lake_centroids_chunk(chunk_start, chunk_end)
            if item[0] not in done_ids
        ]
        rows = [
            {"hylak_id": hylak_id, "pfaf_id": pfaf_id}
            for hylak_id, pfaf_id in provider.lookup_pfaf_chunk(centroids).items()
        ]
        provider.upsert_rows("lake_pfaf", rows)
