"""Comparison grid aggregation: compute Quantile vs PWM exceedance rates.

Reads comparison results from parquet, computes grid-level aggregation,
and outputs 6 parquet files for plotting.

Usage:
    python scripts/comparison_grid_agg.py \
        --comparison-dir /path/to/comparison \
        --sample-file data/comparison/sample_lakes.parquet \
        --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.provider import create_provider

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute comparison grid aggregation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--comparison-dir", type=str, required=True,
        help="Directory containing comparison output parquet files.",
    )
    parser.add_argument(
        "--sample-file", type=str, required=True,
        help="Path to sample_lakes.parquet.",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory. Defaults to comparison-dir.",
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    comparison_dir = Path(args.comparison_dir)
    output_dir = Path(args.output_dir) if args.output_dir else comparison_dir
    sample_file = Path(args.sample_file)

    if not comparison_dir.exists():
        raise FileNotFoundError(f"Comparison dir not found: {comparison_dir}")
    if not sample_file.exists():
        raise FileNotFoundError(f"Sample file not found: {sample_file}")

    log.info("Loading sample IDs from %s", sample_file)
    sample_df = pd.read_parquet(sample_file)
    sample_ids = set(sample_df["hylak_id"].astype(int))
    log.info("Loaded %d sample lake IDs", len(sample_ids))

    config = SourceConfig()
    config._data_dir = Path(args.comparison_dir).parent
    provider = create_provider(config)

    log.info("Fetching comparison.exceedance grid aggregation...")
    agg = provider.fetch_grid_agg(
        "comparison.exceedance",
        args.resolution,
        refresh=args.refresh,
        sample_ids=sample_ids,
        comparison_dir=comparison_dir,
        data_dir=config.data_dir,
    )

    if agg.empty:
        log.warning("No data returned from comparison.exceedance query")
        return

    log.info("Aggregation returned %d grid cells", len(agg))

    output_dir.mkdir(parents=True, exist_ok=True)

    for col in ["q_high_rate", "q_low_rate", "pwm_high_rate", "pwm_low_rate", "diff_high_rate", "diff_low_rate"]:
        if col not in agg.columns:
            log.warning("Column %s not found in aggregation result", col)
            continue

        out_path = output_dir / f"grid_{col}.parquet"
        sub_df = agg[["cell_lat", "cell_lon", "lake_count", col]].copy()
        sub_df.to_parquet(out_path, index=False)
        log.info("Wrote %s (%d rows)", out_path, len(sub_df))

    log.info("Done.")


if __name__ == "__main__":
    main()
