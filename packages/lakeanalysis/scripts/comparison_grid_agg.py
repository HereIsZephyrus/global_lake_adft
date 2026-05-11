"""Comparison grid aggregation: compute Quantile vs PWM exceedance rates.

Computes grid-level aggregation via LakeProvider and caches results
to data/cache/comparison/.  Plot scripts read from cache directly.

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
    uv run python scripts/comparison_grid_agg.py \
        --sample-file data/comparison/sample_lakes.parquet
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakesource.provider import create_provider
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute comparison grid aggregation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sample-file", type=Path, default=DATA_DIR / "comparison" / "sample_lakes.parquet",
        help="Path to sample_lakes.parquet.",
    )
    parser.add_argument(
        "--resolution", type=float, default=0.5,
        help="Grid resolution in degrees.",
    )
    parser.add_argument(
        "--refresh", action="store_true",
        help="Force recompute even if cache exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("comparison_grid_agg")
    load_env()

    sample_file = args.sample_file
    if not sample_file.exists():
        raise FileNotFoundError(f"Sample file not found: {sample_file}")

    log.info("Loading sample IDs from %s", sample_file)
    sample_df = pd.read_parquet(sample_file)
    sample_ids = set(sample_df["hylak_id"].astype(int))
    log.info("Loaded %d sample lake IDs", len(sample_ids))

    source = SourceConfig()
    provider = create_provider(source)

    log.info("Fetching comparison.exceedance grid aggregation...")
    agg = provider.fetch_grid_agg(
        "comparison.exceedance",
        args.resolution,
        refresh=args.refresh,
        sample_ids=sample_ids,
    )

    if agg.empty:
        log.warning("No data returned from comparison.exceedance query")
        return

    log.info("Aggregation returned %d grid cells (cached to data/cache/comparison/)", len(agg))
    log.info("Done.")


if __name__ == "__main__":
    main()