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

from lakeanalysis.artificial.pfaf.runner import PfafRunConfig, run_pfaf
from lakeanalysis.logger import Logger


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


def main() -> None:
    args = parse_args()
    Logger("run_pfaf")
    run_pfaf(PfafRunConfig(limit_id=args.limit_id, chunk_size=args.chunk_size))


if __name__ == "__main__":
    main()
