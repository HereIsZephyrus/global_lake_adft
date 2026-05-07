"""Run the area quality assessment pipeline.

Steps:
  1. Ensure area_quality, area_anomalies tables and area_processed view exist
     in SERIES_DB.
  2. Split the full hylak_id space into chunks of --chunk-size (default 10 000).
  3. For each pending chunk: fetch lake_area, atlas_area, and frozen months from
     SERIES_DB, compute rs_area_mean and rs_area_median per lake (converted from
     m² to km²) from defrozen data, run anomaly filters, then upsert to SERIES_DB:
       - non-anomalous  →  area_quality
       - anomalous      →  area_anomalies (with anomaly_flags bitmask)
  4. Chunks already fully recorded in area_processed (UNION of both tables) are
     skipped automatically, enabling safe resume after an interrupted run.

Usage examples:
    # Full run (chunked, resumable):
    uv run python scripts/run_quality.py

    # Test with only rows where hylak_id < 5000:
    uv run python scripts/run_quality.py --limit-id 5000

    # Adjust chunk size:
    uv run python scripts/run_quality.py --chunk-size 5000
"""

from __future__ import annotations

import argparse

from lakeanalysis.logger import Logger
from lakeanalysis.quality import (
    AreaRatioConfig,
    FlatnessFilterConfig,
    OutsideRangeConfig,
    PenalizedVolatilityConfig,
    ShiftConfig,
)
from lakeanalysis.quality.runner import QualityRunConfig, run_quality


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assess lake area data quality.")
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
    parser.add_argument(
        "--zero-quantile",
        type=float,
        default=0.80,
        metavar="Q",
        help="Zero-quantile filter: quantile position (0-1) at which zero area is flagged (default: 0.80).",
    )
    parser.add_argument(
        "--flat-dominant-ratio-threshold",
        type=float,
        default=0.8,
        metavar="R",
        help="Flatness filter: dominant value frequency ratio threshold.",
    )
    parser.add_argument(
        "--flat-round-digits",
        type=int,
        default=None,
        metavar="N",
        help="Optional rounding digits before flatness statistics.",
    )
    parser.add_argument(
        "--area-ratio-min",
        type=float,
        default=0.1,
        metavar="R",
        help="Area ratio filter: minimum acceptable ratio (default: 0.1).",
    )
    parser.add_argument(
        "--area-ratio-max",
        type=float,
        default=10.0,
        metavar="R",
        help="Area ratio filter: maximum acceptable ratio (default: 10.0).",
    )
    parser.add_argument(
        "--pv-threshold",
        type=float,
        default=0.001,
        metavar="R",
        help="H×CV filter: penalized_volatility <= threshold flags anomaly (default: 0.001).",
    )
    parser.add_argument(
        "--outside-range-tolerance",
        type=float,
        default=0.5,
        metavar="R",
        help="Outside range filter: fractional tolerance beyond observed range (default: 0.5).",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Truncate area_quality and area_anomalies before running (reprocess all chunks).",
    )
    parser.add_argument(
        "--shift-p-value",
        type=float,
        default=0.05,
        metavar="P",
        help="Shift filter: Pettitt significance threshold (default: 0.05).",
    )
    parser.add_argument(
        "--shift-smooth-window",
        type=int,
        default=12,
        metavar="N",
        help="Shift filter: rolling smooth window in months (default: 12).",
    )
    return parser.parse_args()


def build_quality_run_config(args: argparse.Namespace) -> QualityRunConfig:
    return QualityRunConfig(
        limit_id=args.limit_id,
        chunk_size=args.chunk_size,
        zero_quantile=args.zero_quantile,
        flat_config=FlatnessFilterConfig(
            dominant_ratio_threshold=args.flat_dominant_ratio_threshold,
            round_digits=args.flat_round_digits,
        ),
        ratio_config=AreaRatioConfig(
            min_ratio=args.area_ratio_min,
            max_ratio=args.area_ratio_max,
        ),
        pv_config=PenalizedVolatilityConfig(
            pv_threshold=args.pv_threshold,
        ),
        outside_range_config=OutsideRangeConfig(
            tolerance=args.outside_range_tolerance,
        ),
        shift_config=ShiftConfig(
            p_value_thresh=args.shift_p_value,
            smooth_window=args.shift_smooth_window,
        ),
        reset=args.reset,
    )


def main() -> None:
    args = parse_args()
    Logger("run_quality")
    config = build_quality_run_config(args)
    run_quality(config)


if __name__ == "__main__":
    main()
