"""Detect linear interpolation in lake area time series.

For each lake, checks whether the water_area monthly series contains
collinear segments (3+ consecutive points with identical diffs),
after excluding frozen months and zero-area observations.

Segments are classified as:
  - "flat": all diffs ≈ 0 (constant value)
  - "linear": non-zero constant diffs (true linear interpolation)

Only lakes with n_linear_segments > 0 are written to PostgreSQL.
All results are written to parquet as backup.

Usage:
    uv run python scripts/run_interpolation_detect.py
    uv run python scripts/run_interpolation_detect.py --limit-id 5000
    uv run python scripts/run_interpolation_detect.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
from pathlib import Path

from lakeanalysis.logger import Logger
from lakeanalysis.quality.interpolation_runner import (
    InterpolationRunConfig,
    run_interpolation_detect,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect linear interpolation in lake area time series."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Path to data directory (default: /mnt/repo/lake/global_lake_adft/data).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values per chunk (default: 10000).",
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process lakes with hylak_id < N.",
    )
    parser.add_argument(
        "--id-start",
        type=int,
        default=0,
        metavar="N",
        help="Start of hylak_id range (default: 0).",
    )
    parser.add_argument(
        "--id-end",
        type=int,
        default=None,
        metavar="N",
        help="End of hylak_id range (exclusive).",
    )
    parser.add_argument(
        "--min-collinear-points",
        type=int,
        default=4,
        metavar="N",
        help="Minimum consecutive collinear points to flag as interpolation (default: 4).",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip writing to PostgreSQL (only write parquet).",
    )
    parser.add_argument(
        "--output-suffix",
        type=str,
        default="",
        metavar="SUFFIX",
        help="Append suffix to output parquet filename (e.g. '_00' for sharded runs).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_interpolation_detect")

    from lakesource.config import SourceConfig
    data_dir = Path(args.data_dir) if args.data_dir else SourceConfig().data_dir

    run_interpolation_detect(
        InterpolationRunConfig(
            data_dir=data_dir,
            chunk_size=args.chunk_size,
            limit_id=args.limit_id,
            id_start=args.id_start,
            id_end=args.id_end,
            min_collinear_points=args.min_collinear_points,
            no_db=args.no_db,
            output_suffix=args.output_suffix,
        )
    )


if __name__ == "__main__":
    main()
