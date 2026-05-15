"""Compute structural shift labels for all lakes.

Steps:
  1. Run batch Engine with ShiftLabelsCalculator to produce area_shift_labels.parquet.
  2. Upsert rows from parquet into the provider's area_shift_labels table.
  3. Sync labels to area_quality / area_anomalies:
       - degraded lakes -> area_anomalies with FLAG_SHIFT
       - intermittent/stable with only FLAG_SHIFT -> area_quality

Usage examples:
    # Full run:
    uv run python scripts/compute_shift_labels.py

    # Test with limit:
    uv run python scripts/compute_shift_labels.py --limit-id 100000

    # Dry run (no DB changes):
    uv run python scripts/compute_shift_labels.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakeanalysis.batch import Engine, RangeFilter
from lakeanalysis.batch.io import build_provider_batch_reader, build_provider_batch_writer
from lakeanalysis.logger import Logger
from lakeanalysis.quality import sync_shift_to_anomalies, upsert_shift_labels_from_parquet
from lakeanalysis.quality.shift_labels_calculator import ShiftLabelsCalculator
from lakesource.config import Backend, SourceConfig
from lakesource.provider.factory import create_provider

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute structural shift labels for lakes.")
    parser.add_argument(
        "--backend",
        type=str,
        choices=[b.value for b in Backend],
        default=None,
        help="Data backend (default: from DATA_BACKEND env or parquet).",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Parquet data directory when backend=parquet.",
    )
    parser.add_argument(
        "--output-parquet",
        type=str,
        default=None,
        help="Output parquet path for area_shift_labels (default: {data_dir}/area_shift_labels.parquet).",
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process lakes with hylak_id < N.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Chunk size for batch processing (default: 10000).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate area_shift_labels parquet before computing.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip DB writes during sync step.",
    )
    parser.add_argument(
        "--skip-compute",
        action="store_true",
        help="Skip batch computation step (use existing parquet).",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip sync step (only compute and upsert).",
    )
    parser.add_argument(
        "--sync-backend",
        type=str,
        choices=[b.value for b in Backend],
        default=None,
        help="Backend for sync step (default: same as --backend).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("compute_shift_labels")

    compute_backend = Backend(args.backend) if args.backend else None
    sync_backend = Backend(args.sync_backend) if args.sync_backend else compute_backend

    compute_config = SourceConfig(
        backend=compute_backend,
        data_dir=Path(args.data_dir) if args.data_dir else None,
    )
    compute_provider = create_provider(compute_config)
    data_dir = compute_config.data_dir
    output_parquet = Path(args.output_parquet) if args.output_parquet else data_dir / "area_shift_labels.parquet"

    if args.reset:
        log.info("Resetting area_shift_labels parquet...")
        compute_provider.truncate_table("area_shift_labels")
        if output_parquet.exists():
            output_parquet.unlink()

    if not args.skip_compute:
        log.info("Step 1: Running batch Engine to compute shift labels...")
        reader = build_provider_batch_reader(compute_config, done_table="area_shift_labels")
        writer = build_provider_batch_writer(compute_config)
        calculator = ShiftLabelsCalculator()
        engine = Engine(
            reader=reader,
            writer=writer,
            calculator=calculator,
            algorithm="shift_labels",
            lake_filter=RangeFilter(end=args.limit_id) if args.limit_id is not None else None,
            chunk_size=args.chunk_size,
        )
        engine.run()
        log.info("Batch computation complete. Output: %s", output_parquet)

    if not args.skip_sync:
        sync_config = SourceConfig(backend=sync_backend)
        sync_provider = create_provider(sync_config)

        log.info("Step 2: Upserting shift labels from parquet to DB (backend=%s)...", sync_backend)
        upsert_shift_labels_from_parquet(output_parquet, sync_provider)

        log.info("Step 3: Syncing labels to area_quality / area_anomalies (backend=%s)...", sync_backend)
        sync_shift_to_anomalies(sync_provider, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
