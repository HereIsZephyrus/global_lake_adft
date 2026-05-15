"""Compute lake-pair similarity (Pearson + ACF cosine) for af_nearest pairs with topo_level>8.

Steps:
  1. Fetch af_nearest (topo_level > 8) and lake_area for all involved hylak_ids from SERIES_DB.
  2. For each pair (hylak_id, nearest_id), align water_area series and compute pearson_r,
     acf_cos_sim (12-month delay), and n_common.
  3. Write results to data/similarity/similarity.csv.
  4. Optionally plot distributions and scatter (--plot / --plot-only).

Usage:
  uv run python scripts/run_similarity.py
  uv run python scripts/run_similarity.py --limit-pairs 500
  uv run python scripts/run_similarity.py --plot
  uv run python scripts/run_similarity.py --plot-only
"""

from __future__ import annotations

import argparse
from pathlib import Path

from lakeanalysis.artificial.similarity.runner import (
    SimilarityRunConfig,
    run_similarity,
    show_similarity_plots,
)
from lakeanalysis.logger import Logger

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "similarity"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for run_similarity."""
    parser = argparse.ArgumentParser(
        description="Compute Pearson and ACF cosine similarity for lake pairs (topo_level>8)."
    )
    parser.add_argument(
        "--limit-pairs",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N pairs (for testing).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate matplotlib plots after computation.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Load from similarity.csv and plot only; skip computation.",
    )
    return parser.parse_args()


def main() -> None:
    """Entry point: parse args, run pipeline or plot-only."""
    args = parse_args()
    Logger("run_similarity")
    if args.plot_only:
        show_similarity_plots(DATA_DIR)
    else:
        run_similarity(
            SimilarityRunConfig(
                data_dir=DATA_DIR,
                limit_pairs=args.limit_pairs,
                show_plot=args.plot,
            )
        )


if __name__ == "__main__":
    main()
