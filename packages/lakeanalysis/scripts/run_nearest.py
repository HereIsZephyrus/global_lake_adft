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
import logging
from pathlib import Path

from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

from lakesource.postgres import series_db
from lakeanalysis.pfaf import compute_nearest_naturals, ensure_af_nearest_table, upsert_af_nearest


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


def run(limit_id: int | None = None, max_area_ratio: float = 2.0) -> list[dict]:
    """Execute the nearest-natural-lake pipeline.

    Args:
        limit_id: If given, only type>1 lakes with hylak_id < limit_id are
            searched (type-1 index always loads the full dataset).
        max_area_ratio: Maximum ratio between matched lake areas.  Defaults
            to 10.0.  Pass ``float("inf")`` to disable the area constraint.

    Returns:
        List of result dicts written to af_nearest.
    """
    log.info(
        "Starting nearest-natural pipeline, limit_id=%s, max_area_ratio=%.1f",
        limit_id if limit_id is not None else "none",
        max_area_ratio,
    )

    with series_db.connection_context() as conn:
        ensure_af_nearest_table(conn)
        rows = compute_nearest_naturals(conn, limit_id=limit_id, max_area_ratio=max_area_ratio)

    matched = sum(1 for r in rows if r["nearest_id"] is not None)
    log.info(
        "Search done: %d/%d type>1 lakes matched a natural lake",
        matched,
        len(rows),
    )

    with series_db.connection_context() as conn:
        log.info("Upserting %d row(s) into af_nearest...", len(rows))
        upsert_af_nearest(conn, rows)

    log.info("Pipeline complete.")
    return rows


def main() -> None:
    args = parse_args()
    Logger("run_nearest")
    run(limit_id=args.limit_id, max_area_ratio=args.max_area_ratio)


if __name__ == "__main__":
    main()
