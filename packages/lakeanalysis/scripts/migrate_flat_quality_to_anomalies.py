"""Reclassify flat-series lakes from area_quality to area_anomalies.

This script applies the same flatness filters used by ``run_quality`` and moves
matching hylak_ids from ``area_quality`` to ``area_anomalies``.

Usage examples:
    # Inspect how many lakes would be moved:
    uv run python scripts/migrate_flat_quality_to_anomalies.py --dry-run

    # Execute migration:
    uv run python scripts/migrate_flat_quality_to_anomalies.py

    # Test run with id cap and custom thresholds:
    uv run python scripts/migrate_flat_quality_to_anomalies.py \
        --limit-id 50000 \
        --flat-dominant-ratio-threshold 0.85
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakeanalysis.dbconnect import (
    ChunkedLakeProcessor,
    ensure_area_anomalies_table,
    ensure_area_quality_table,
    fetch_lake_area_chunk,
    move_area_quality_to_anomalies,
    series_db,
)
from lakeanalysis.logger import Logger
from lakeanalysis.quality import FlatnessFilterConfig, classify_area_anomaly, compute_median_area

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move flat-series lakes from area_quality to area_anomalies."
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only scan hylak_id < N.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values scanned per chunk.",
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
    parser.add_argument(
        "--move-batch-size",
        type=int,
        default=5000,
        metavar="N",
        help="Batch size for DB migration operations.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only detect candidate hylak_id values, do not migrate.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        metavar="PATH",
        help="Optional output .txt path; write candidate hylak_id values as one space-separated line.",
    )
    return parser.parse_args()


def _iter_batches(items: list[int], batch_size: int) -> list[list[int]]:
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def _write_candidates(output_path: Path, candidates: set[int]) -> None:
    """Write candidate ids to output_path as one space-separated line."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    text = " ".join(str(hylak_id) for hylak_id in sorted(candidates))
    output_path.write_text(text, encoding="utf-8")


def run(
    limit_id: int | None,
    chunk_size: int,
    flat_config: FlatnessFilterConfig,
    move_batch_size: int,
    dry_run: bool,
    output_path: Path | None,
) -> None:
    """Detect flat-series lakes in area_quality and move them to area_anomalies."""
    with series_db.connection_context() as conn:
        ensure_area_quality_table(conn)
        ensure_area_anomalies_table(conn)

    processor = ChunkedLakeProcessor(
        series_db=series_db,
        chunk_size=chunk_size,
        done_table="area_quality",
    )

    candidates: list[int] = []
    candidate_set: set[int] = set()
    dominant_hits = 0

    if output_path is not None:
        _write_candidates(output_path, candidate_set)
        log.info("Initialized output file: %s", output_path)

    all_chunks = list(processor.iter_all_chunks(limit_id=limit_id))
    for idx, (chunk_start, chunk_end) in enumerate(all_chunks, start=1):
        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
        if not lake_frames:
            if output_path is not None:
                _write_candidates(output_path, candidate_set)
                log.info(
                    "[%d/%d] wrote output after empty chunk %d-%d: %d candidate(s)",
                    idx,
                    len(all_chunks),
                    chunk_start,
                    chunk_end - 1,
                    len(candidate_set),
                )
            continue

        for hylak_id, df in lake_frames.items():
            rs_area_median = compute_median_area(df) / 1_000_000
            decision = classify_area_anomaly(df, rs_area_median, flat_config)
            if not bool(decision["is_flat"]):
                continue
            candidates.append(int(hylak_id))
            candidate_set.add(int(hylak_id))
            if bool(decision["is_flat_dominant"]):
                dominant_hits += 1

        if output_path is not None:
            _write_candidates(output_path, candidate_set)
            log.info(
                "[%d/%d] wrote output after chunk %d-%d: %d candidate(s)",
                idx,
                len(all_chunks),
                chunk_start,
                chunk_end - 1,
                len(candidate_set),
            )

        log.info(
            "[%d/%d] scanned chunk %d-%d: %d candidate(s) so far",
            idx,
            len(all_chunks),
            chunk_start,
            chunk_end - 1,
            len(candidates),
        )

    unique_candidates = sorted(set(candidates))
    log.info(
        "Detected %d candidate lakes (dominant_hit=%d)",
        len(unique_candidates),
        dominant_hits,
    )
    if output_path is not None:
        log.info("Final output candidate count: %d (%s)", len(unique_candidates), output_path)
    if not unique_candidates:
        return

    if dry_run:
        log.info("Dry run only, no DB write.")
        return

    total_moved = 0
    for batch in _iter_batches(unique_candidates, move_batch_size):
        with series_db.connection_context() as conn:
            moved = move_area_quality_to_anomalies(conn, batch)
        total_moved += moved
        log.info("Moved batch: %d rows (total=%d)", moved, total_moved)

    log.info(
        "Migration complete: moved %d / %d candidate lakes",
        total_moved,
        len(unique_candidates),
    )


def main() -> None:
    args = parse_args()
    Logger("migrate_flat_quality_to_anomalies")
    flat_config = FlatnessFilterConfig(
        dominant_ratio_threshold=args.flat_dominant_ratio_threshold,
        round_digits=args.flat_round_digits,
    )
    run(
        limit_id=args.limit_id,
        chunk_size=args.chunk_size,
        flat_config=flat_config,
        move_batch_size=args.move_batch_size,
        dry_run=args.dry_run,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
