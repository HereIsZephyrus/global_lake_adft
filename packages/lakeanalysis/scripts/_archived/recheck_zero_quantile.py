"""Recheck lakes flagged by median_zero using a higher quantile threshold.

For lakes currently in area_anomalies with the zero_quantile (bit 1) flag,
recompute the quantile area at the new threshold.  If the new quantile is
non-zero, re-run all filters and move lakes that are no longer anomalous
to area_quality, or update their anomaly_flags if other filters still
trigger.

Two-phase approach for speed:
  Phase 1: SQL-side PERCENTILE_CONT to find lakes where the new quantile
           is non-zero (skips ~60% of candidates without fetching raw data).
  Phase 2: Fetch full time-series only for those lakes, run all filters.

Usage:
    # Dry run (stats only):
    uv run python scripts/recheck_zero_quantile.py --dry-run

    # Execute with default p80:
    uv run python scripts/recheck_zero_quantile.py

    # Use p90 instead:
    uv run python scripts/recheck_zero_quantile.py --zero-quantile 0.90

    # Custom batch size:
    uv run python scripts/recheck_zero_quantile.py --batch-size 10000
"""

from __future__ import annotations

import argparse

from lakeanalysis.logger import Logger
from lakeanalysis.quality.maintenance_runner import (
    RecheckZeroQuantileConfig,
    run_recheck_zero_quantile,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recheck zero-quantile flagged lakes with a higher quantile threshold.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--zero-quantile",
        type=float,
        default=0.80,
        metavar="Q",
        help="Quantile position (0-1) for the zero check (default: 0.80).",
    )
    parser.add_argument("--batch-size", type=int, default=10_000, metavar="N")
    parser.add_argument("--dry-run", action="store_true", help="Stats only, no writes.")
    return parser.parse_args()


def main() -> None:
    Logger("recheck_zero_quantile")
    args = parse_args()
    run_recheck_zero_quantile(
        RecheckZeroQuantileConfig(
            zero_quantile=args.zero_quantile,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )
    )


if __name__ == "__main__":
    main()
