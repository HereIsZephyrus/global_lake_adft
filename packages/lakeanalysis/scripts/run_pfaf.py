"""Run the Pfafstetter basin ID lookup pipeline.

Steps:
  1. Ensure lake_pfaf table exists in SERIES_DB.
  2. Split the full hylak_id space into chunks of --chunk-size (default 10 000).
  3. For each pending chunk: fetch centroids from SERIES_DB, run the two-pass
     spatial join against ALTAS_DB, upsert results back to SERIES_DB.
  4. Chunks already fully recorded in lake_pfaf are skipped automatically,
     enabling safe resume after an interrupted run.

Usage examples:
    # Full run (chunked, resumable):
    uv run python scripts/run_pfaf.py

    # Test with only rows where hylak_id < 5000:
    uv run python scripts/run_pfaf.py --limit-id 5000

    # Adjust chunk size:
    uv run python scripts/run_pfaf.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

from lakesource.postgres import ChunkedLakeProcessor, atlas_db, series_db
from lakeanalysis.pfaf import (
    ensure_lake_pfaf_table,
    fetch_lake_centroids_chunk,
    lookup_pfaf_chunk,
    upsert_lake_pfaf,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Look up Pfafstetter basin IDs for lakes.")
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process rows with hylak_id < N (for testing).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values processed per chunk (default: 10000).",
    )
    return parser.parse_args()


def run(limit_id: int | None = None, chunk_size: int = 10_000) -> None:
    """Execute the pfaf lookup pipeline in resumable chunks.

    Each chunk of ``chunk_size`` consecutive hylak_id values is processed
    independently.  Results are upserted to ``lake_pfaf`` after every chunk so
    that a crashed run can resume from where it left off without reprocessing
    completed chunks.

    Args:
        limit_id: If given, only lakes with hylak_id < limit_id are processed.
        chunk_size: Number of hylak_id values per processing chunk.
    """
    log.info("Starting pfaf lookup pipeline, limit_id=%s, chunk_size=%d", limit_id, chunk_size)

    with series_db.connection_context() as series_conn:
        ensure_lake_pfaf_table(series_conn)

    processor = ChunkedLakeProcessor(series_db, chunk_size=chunk_size)

    def process_chunk(chunk_start: int, chunk_end: int) -> dict[int, int | None]:
        with series_db.connection_context() as s_conn:
            centroids = fetch_lake_centroids_chunk(s_conn, chunk_start, chunk_end)
        with atlas_db.connection_context() as a_conn:
            return lookup_pfaf_chunk(a_conn, centroids)

    def upsert_chunk(mapping: dict[int, int | None]) -> None:
        with series_db.connection_context() as s_conn:
            upsert_lake_pfaf(s_conn, mapping)

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk, limit_id=limit_id)


def main() -> None:
    args = parse_args()
    Logger("run_pfaf")
    run(limit_id=args.limit_id, chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
