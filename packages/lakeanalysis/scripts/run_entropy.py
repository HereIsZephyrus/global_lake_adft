"""Run the Apportionment Entropy (AE) computation pipeline.

Steps:
  1. Ensure the entropy table exists in SERIES_DB.
  2. Split the full hylak_id space into chunks of --chunk-size (default 10 000).
  3. For each pending chunk: fetch lake_area from SERIES_DB, compute AE metrics,
     persist results to data/entropy/ as parquet, then upsert to SERIES_DB.
  4. Chunks already fully recorded in entropy are skipped automatically,
     enabling safe resume after an interrupted run.
  5. Optionally display matplotlib exploration plots (--plot).

Usage examples:
    # Full run (chunked, resumable):
    uv run python scripts/run_entropy.py

    # Test with only rows where id < 5000:
    uv run python scripts/run_entropy.py --limit-id 5000

    # Test + show plots:
    uv run python scripts/run_entropy.py --limit-id 5000 --plot

    # Plot only (load all parquet from data/entropy, no recompute):
    uv run python scripts/run_entropy.py --plot-only

    # Update amplitude only (re-fetch STL from lake_info; no AE recompute):
    uv run python scripts/run_entropy.py --update-amplitude-only
    uv run python scripts/run_entropy.py --update-amplitude-only --plot

    # Adjust chunk size:
    uv run python scripts/run_entropy.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
from pathlib import Path

from lakeanalysis.logger import Logger
from lakeanalysis.entropy.service import (
    EntropyRunConfig,
    run_entropy,
    run_update_amplitude_only,
)
from lakeanalysis.entropy.runner import show_entropy_plots

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "entropy"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute Apportionment Entropy for lake_area data.")
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process rows with id < N (for testing).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values processed per chunk (default: 10000).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Show matplotlib exploration plots after computation.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Only load from data/entropy and plot; skip computation.",
    )
    parser.add_argument(
        "--update-amplitude-only",
        action="store_true",
        help="Only refresh mean_seasonal_amplitude (CV = annual_means_std/mean_area from lake_info); update parquet and DB.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_entropy")
    if args.plot_only:
        show_entropy_plots(DATA_DIR, limit_id=None)
    elif args.update_amplitude_only:
        run_update_amplitude_only(DATA_DIR, show_plot=args.plot)
    else:
        run_entropy(
            EntropyRunConfig(
                data_dir=DATA_DIR,
                limit_id=args.limit_id,
                chunk_size=args.chunk_size,
                show_plot=args.plot,
            )
        )


if __name__ == "__main__":
    main()
