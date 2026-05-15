"""Recompute PV filter only, moving lakes between area_quality and area_anomalies.

Python-side computation: fetch raw data from PostgreSQL, filter frozen and
compute PV (H×CV) in Python.  Avoids SQL-side query plan instability with
NOT EXISTS / DELETE WHERE EXISTS on the large anomaly table.

Usage:
    # Dry run (stats only):
    uv run python scripts/recompute_pv.py --dry-run

    # Execute:
    uv run python scripts/recompute_pv.py

    # Custom chunk size:
    uv run python scripts/recompute_pv.py --chunk-size 5000

    # Limit to hylak_id < 50000:
    uv run python scripts/recompute_pv.py --dry-run --limit-id 50000
"""

from __future__ import annotations

import argparse

from lakeanalysis.logger import Logger
from lakeanalysis.quality.maintenance_runner import (
    RecomputePvConfig,
    run_recompute_pv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute PV filter and update area_quality / area_anomalies.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--chunk-size", type=int, default=5_000, metavar="N")
    parser.add_argument("--start-id", type=int, default=0, metavar="ID", help="Skip chunks with hylak_id < ID.")
    parser.add_argument("--limit-id", type=int, default=None, metavar="ID", help="Only process hylak_id < ID.")
    parser.add_argument("--dry-run", action="store_true", help="Stats only, no writes.")
    return parser.parse_args()


def main() -> None:
    Logger("recompute_pv")
    args = parse_args()
    run_recompute_pv(
        RecomputePvConfig(
            chunk_size=args.chunk_size,
            start_id=args.start_id,
            limit_id=args.limit_id,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
