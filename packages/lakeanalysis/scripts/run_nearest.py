"""Run the nearest-natural-lake search pipeline.

Steps:
  1. Ensure the af_nearest table exists in SERIES_DB.
  2. Load all type-1 lakes and build the in-memory Pfafstetter skip-list index.
  3. For each type>1 lake, find the topologically nearest type-1 lake by
     walking from the finest Pfafstetter level (lev11) upward until a match
     is found; geographic distance breaks ties within the same level.
  4. Upsert (hylak_id, lake_type, nearest_id, topo_level) into af_nearest.

Usage examples:
    # Full run:
    uv run python scripts/run_nearest.py

    # Test with only rows where hylak_id < 5000:
    uv run python scripts/run_nearest.py --limit-id 5000
"""

from __future__ import annotations

import argparse

from lakeanalysis.artificial.pfaf.nearest_runner import NearestRunConfig, run_nearest
from lakeanalysis.logger import Logger


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Find the nearest type-1 (natural) lake for each type>1 lake."
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process type>1 rows with hylak_id < N (for testing).",
    )
    parser.add_argument(
        "--max-area-ratio",
        type=float,
        default=10.0,
        metavar="R",
        help=(
            "Maximum allowed area ratio between matched lakes "
            "(default: 10.0, i.e. neither lake more than 10× the other)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_nearest")
    run_nearest(
        NearestRunConfig(
            limit_id=args.limit_id,
            max_area_ratio=args.max_area_ratio,
        )
    )


if __name__ == "__main__":
    main()
