"""Run the area quality assessment pipeline.

Steps:
  1. Ensure area_quality, area_anomalies tables and area_processed view exist
     in SERIES_DB.
  2. Split the full hylak_id space into chunks of --chunk-size (default 10 000).
  3. For each pending chunk: fetch lake_area and atlas_area from SERIES_DB,
    compute rs_area_mean and rs_area_median per lake (converted from m² to km²),
    run flatness filters, then upsert to SERIES_DB:
      - non-anomalous  →  area_quality
      - median-zero or flat-series anomaly  →  area_anomalies
  4. Chunks already fully recorded in area_processed (UNION of both tables) are
     skipped automatically, enabling safe resume after an interrupted run.

Usage examples:
    # Full run (chunked, resumable):
    uv run python scripts/run_quality.py

    # Test with only rows where hylak_id < 5000:
    uv run python scripts/run_quality.py --limit-id 5000

    # Adjust chunk size:
    uv run python scripts/run_quality.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
import logging

from lakesource.postgres import (
    ChunkedLakeProcessor,
    ensure_area_anomalies_table,
    ensure_area_quality_table,
    fetch_atlas_area_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_area_anomalies,
    upsert_area_quality,
)
from lakeanalysis.logger import Logger
from lakeanalysis.quality import FlatnessFilterConfig, classify_area_anomaly, compute_mean_area, compute_median_area

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assess lake area data quality.")
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
    parser.add_argument(
        "--flat-dominant-ratio-threshold",
        type=float,
        default=0.8,
        metavar="R",
        help="Flatness filter 1: dominant value frequency ratio threshold.",
    )
    parser.add_argument(
        "--flat-round-digits",
        type=int,
        default=None,
        metavar="N",
        help="Optional rounding digits before flatness statistics.",
    )
    return parser.parse_args()


def run(
    limit_id: int | None = None,
    chunk_size: int = 10_000,
    flat_config: FlatnessFilterConfig | None = None,
) -> None:
    """Execute the area quality pipeline in resumable chunks.

    Each chunk of ``chunk_size`` consecutive hylak_id values is processed
    independently and upserted directly to SERIES_DB:

    - Non-anomalous lakes  →  ``area_quality``
    - Median-zero / flat-series anomalies  →  ``area_anomalies``

    The ``area_processed`` view (UNION of both tables) is used as the checkpoint
    table so that all processed lakes count toward chunk completion, enabling
    safe resume after an interrupted run.

    Args:
        limit_id: If given, only lakes with hylak_id < limit_id are processed.
        chunk_size: Number of hylak_id values per processing chunk.
        flat_config: Flatness filter configuration.
    """
    if flat_config is None:
        flat_config = FlatnessFilterConfig()

    log.info(
        "Starting area quality pipeline, limit_id=%s, chunk_size=%d, "
        "flat_dominant_ratio_threshold=%.3f, flat_round_digits=%s",
        limit_id,
        chunk_size,
        flat_config.dominant_ratio_threshold,
        flat_config.round_digits,
    )

    with series_db.connection_context() as conn:
        ensure_area_quality_table(conn)
        ensure_area_anomalies_table(conn)

    processor = ChunkedLakeProcessor(series_db, chunk_size=chunk_size, done_table="area_processed")

    def process_chunk(chunk_start: int, chunk_end: int) -> dict[str, list[dict]]:
        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            atlas_areas = fetch_atlas_area_chunk(conn, chunk_start, chunk_end)

        normal: list[dict] = []
        anomalies: list[dict] = []
        n_median_zero = 0
        n_flat = 0

        for hylak_id, df in lake_frames.items():
            rs_area_median = compute_median_area(df) / 1_000_000
            rs_area_mean = compute_mean_area(df) / 1_000_000
            decision = classify_area_anomaly(df, rs_area_median, flat_config)
            row = {
                "hylak_id": hylak_id,
                "rs_area_mean": rs_area_mean,
                "rs_area_median": rs_area_median,
                "atlas_area": atlas_areas.get(hylak_id, 0.0),
            }
            if bool(decision["is_anomalous"]):
                anomalies.append(row)
                if bool(decision["is_median_zero"]):
                    n_median_zero += 1
                if bool(decision["is_flat"]):
                    n_flat += 1
            else:
                normal.append(row)

        log.debug(
            "chunk [%d, %d): %d normal, %d anomalous (median_zero=%d, flat=%d)",
            chunk_start,
            chunk_end,
            len(normal),
            len(anomalies),
            n_median_zero,
            n_flat,
        )
        return {"normal": normal, "anomalies": anomalies}

    def upsert_chunk(result: dict[str, list[dict]]) -> None:
        with series_db.connection_context() as conn:
            if result["normal"]:
                upsert_area_quality(conn, result["normal"])
            if result["anomalies"]:
                upsert_area_anomalies(conn, result["anomalies"])

    processor.run(process_fn=process_chunk, upsert_fn=upsert_chunk, limit_id=limit_id)


def main() -> None:
    args = parse_args()
    Logger("run_quality")
    flat_config = FlatnessFilterConfig(
        dominant_ratio_threshold=args.flat_dominant_ratio_threshold,
        round_digits=args.flat_round_digits,
    )
    run(limit_id=args.limit_id, chunk_size=args.chunk_size, flat_config=flat_config)


if __name__ == "__main__":
    main()
