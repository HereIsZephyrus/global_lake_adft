"""Run PWM extreme batch computation via unified batch framework."""

from __future__ import annotations

import argparse
import logging
import os

os.environ.setdefault("NUMBA_NUM_THREADS", "1")

from lakesource.config import SourceConfig
from lakeanalysis.batch import (
    Engine,
    RangeFilter,
    build_provider_batch_reader,
    build_provider_batch_writer,
)
from lakeanalysis.batch.dataset import Dataset
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run PWM extreme batch computation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--limit-id", type=int, default=None)
    parser.add_argument("--id-start", type=int, default=0)
    parser.add_argument("--id-end", type=int, default=None)
    parser.add_argument("--min-valid-per-month", type=int, default=None)
    parser.add_argument("--min-valid-observations", type=int, default=None)
    parser.add_argument("--io-budget", type=int, default=4, help="Max concurrent DB IO workers.")
    parser.add_argument("--method", default="stl", choices=["stl", "legacy"],
                       help="Decomposition method.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_pwm_extreme")

    source_config = SourceConfig()
    id_start = args.id_start
    id_end = args.id_end
    if args.limit_id is not None:
        id_end = args.limit_id if id_end is None else min(id_end, args.limit_id)

    range_filter = None
    if id_start > 0 or id_end is not None:
        range_filter = RangeFilter(start=id_start, end=id_end)

    dataset = Dataset(source_config, lake_filter=range_filter)

    reader = build_provider_batch_reader(
        source_config,
        done_table="pwm_extreme_run_status",
        done_requires_status=True,
    )
    writer = build_provider_batch_writer(source_config, ensure_tables=["pwm_extreme"])
    calculator = CalculatorFactory.create(
        "pwm_extreme",
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
        method=args.method,
    )

    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        algorithm="pwm_extreme",
        lake_filter=dataset.as_filter(),
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
