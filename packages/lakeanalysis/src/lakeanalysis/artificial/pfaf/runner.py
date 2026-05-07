"""Application runner for the Pfafstetter lookup pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lakesource.postgres import ChunkedLakeProcessor, atlas_db, series_db

from .lookup import fetch_lake_centroids_chunk, lookup_pfaf_chunk
from .store import ensure_lake_pfaf_table, upsert_lake_pfaf

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

    with series_db.connection_context() as series_conn:
        ensure_lake_pfaf_table(series_conn)

    processor = ChunkedLakeProcessor(series_db, chunk_size=config.chunk_size)

    def process_chunk(chunk_start: int, chunk_end: int) -> dict[int, int | None]:
        with series_db.connection_context() as s_conn:
            centroids = fetch_lake_centroids_chunk(s_conn, chunk_start, chunk_end)
        with atlas_db.connection_context() as a_conn:
            return lookup_pfaf_chunk(a_conn, centroids)

    def upsert_chunk(mapping: dict[int, int | None]) -> None:
        with series_db.connection_context() as s_conn:
            upsert_lake_pfaf(s_conn, mapping)

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk, limit_id=config.limit_id)
