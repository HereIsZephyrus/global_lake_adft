"""Run chunked DB batch execution for quantile anomaly transition workflow."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakeanalysis.logger import Logger
from lakeanalysis.quantile import (
    QuantileBatchConfig,
    run_quantile_batch,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "quantile"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for batch execution."""
    parser = argparse.ArgumentParser(
        description="Run quantile anomaly transition workflow on DB data in chunks.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="Number of hylak_id values per chunk.",
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        help="Optional upper bound; only process hylak_id < limit_id.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DATA_DIR,
        help="Root directory for summary cache and summary plots.",
    )
    parser.add_argument(
        "--min-valid-per-month",
        type=int,
        default=None,
        help="Minimum valid observations required for each calendar month.",
    )
    parser.add_argument(
        "--min-valid-observations",
        type=int,
        default=None,
        help="Minimum valid observations required overall.",
    )
    parser.add_argument(
        "--no-summary-cache",
        action="store_true",
        help="Skip local summary cache generation after batch execution.",
    )
    parser.add_argument(
        "--no-summary-plot",
        action="store_true",
        help="Skip global summary plot generation.",
    )
    return parser.parse_args()


def run(args: argparse.Namespace):
    """Execute chunked DB batch run with resumable status tracking."""
    config = QuantileBatchConfig(
        output_root=args.output_root,
        chunk_size=args.chunk_size,
        limit_id=args.limit_id,
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
        build_summary_cache=not args.no_summary_cache,
        plot_summary=not args.no_summary_plot,
    )
    return run_quantile_batch(config)


def main() -> None:
    """CLI entry point."""
    args = parse_args()
    Logger("run_quantile_batch")
    logging.getLogger("matplotlib").setLevel(logging.WARNING)
    logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
    report = run(args)
    log.info(
        "Batch complete: chunks processed=%d skipped=%d total=%d; lakes source=%d skip=%d "
        "success=%d error=%d",
        report.processed_chunks,
        report.skipped_chunks,
        report.total_chunks,
        report.source_lakes,
        report.skipped_lakes,
        report.success_lakes,
        report.error_lakes,
    )
    if report.cache_paths:
        for name, path in report.cache_paths.items():
            log.info("summary cache %s: %s", name, path)
    if report.plot_paths:
        for name, path in report.plot_paths.items():
            log.info("summary plot %s: %s", name, path)


if __name__ == "__main__":
    main()
