"""Run quantile batch computation via unified batch framework."""

from __future__ import annotations

import argparse
import logging

from lakeanalysis.batch import Engine, RangeFilter
from lakeanalysis.batch.io import DBIOFactory
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run quantile batch computation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--limit-id", type=int, default=None)
    parser.add_argument("--id-start", type=int, default=0)
    parser.add_argument("--id-end", type=int, default=None)
    parser.add_argument("--min-valid-per-month", type=int, default=None)
    parser.add_argument("--min-valid-observations", type=int, default=None)
    parser.add_argument("--io-budget", type=int, default=4, help="Max concurrent DB IO workers.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_quantile")

    factory = DBIOFactory()
    reader = factory.create_reader("quantile", workflow_version="monthly-transition-v1")
    writer = factory.create_writer("quantile")
    calculator = CalculatorFactory.create(
        "quantile",
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
    )

    lake_filter = None
    if args.id_start > 0 or args.id_end is not None:
        lake_filter = RangeFilter(start=args.id_start, end=args.id_end)

    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        lake_filter=lake_filter,
        chunk_size=args.chunk_size,
        limit_id=args.limit_id,
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
