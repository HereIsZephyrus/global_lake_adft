"""Run EOT batch computation via unified batch framework."""

from __future__ import annotations

import argparse
import logging

from lakesource.config import SourceConfig
from lakesource.provider import create_provider

from lakeanalysis.batch import Engine, RangeFilter
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run EOT batch computation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--limit-id", type=int, default=None)
    parser.add_argument("--id-start", type=int, default=0)
    parser.add_argument("--id-end", type=int, default=None)
    parser.add_argument(
        "--tail",
        choices=["high", "low", "both"],
        default="both",
    )
    parser.add_argument(
        "--threshold-quantiles",
        nargs="+",
        type=float,
        default=[0.95, 0.98],
    )
    parser.add_argument("--io-budget", type=int, default=4, help="Max concurrent DB IO workers.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_eot")

    tails = ["high", "low"] if args.tail == "both" else [args.tail]

    provider = create_provider(SourceConfig())
    calculator = CalculatorFactory.create(
        "eot",
        tails=tails,
        quantiles=args.threshold_quantiles,
    )

    id_start = args.id_start
    id_end = args.id_end
    if args.limit_id is not None:
        id_end = args.limit_id if id_end is None else min(id_end, args.limit_id)

    lake_filter = None
    if id_start > 0 or id_end is not None:
        lake_filter = RangeFilter(start=id_start, end=id_end)

    engine = Engine(
        provider=provider,
        calculator=calculator,
        algorithm="eot",
        lake_filter=lake_filter,
        chunk_size=args.chunk_size,
        io_budget=args.io_budget,
    )

    report = engine.run()
    if report:
        log.info(
            "Done: chunks=%d/%d success=%d error=%d",
            report.processed_chunks,
            report.total_chunks,
            report.success_lakes,
            report.error_lakes,
        )


if __name__ == "__main__":
    main()
