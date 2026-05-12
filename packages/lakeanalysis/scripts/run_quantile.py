"""Run quantile batch computation via unified batch framework."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakeanalysis.batch import (
    Engine,
    RangeFilter,
    build_provider_batch_reader,
    build_provider_batch_writer,
)
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger
from lakeanalysis.quantile import QuantileServiceConfig, run_single_lake_service

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
    parser.add_argument("--method", default="stl", choices=["stl", "legacy"],
                       help="Decomposition method.")
    return parser.parse_args()


def run(args: argparse.Namespace) -> dict[str, Path]:
    """Run the legacy single-lake CSV workflow used by tests.

    This preserves the historical script-level interface while the default CLI
    entrypoint continues to use the batch engine.
    """
    if getattr(args, "csv", None) is None:
        raise ValueError("args.csv is required for single-lake runner mode")
    if getattr(args, "hylak_id", None) is None:
        raise ValueError("args.hylak_id is required for single-lake runner mode")

    series_df = pd.read_csv(args.csv)
    lake_df = series_df.loc[series_df["hylak_id"] == args.hylak_id].copy()
    if lake_df.empty:
        raise ValueError(f"No rows found for hylak_id={args.hylak_id}")

    config = QuantileServiceConfig(
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
    )
    result = run_single_lake_service(
        lake_df,
        hylak_id=args.hylak_id,
        config=config,
    )

    output_root = Path(args.output_root)
    lake_dir = output_root / str(args.hylak_id)
    lake_dir.mkdir(parents=True, exist_ok=True)

    month_labels_path = lake_dir / "month_labels.csv"
    result.labels_df.to_csv(month_labels_path, index=False)

    return {
        "output_root": output_root,
        "lake_dir": lake_dir,
        "month_labels": month_labels_path,
    }


def main() -> None:
    args = parse_args()
    Logger("run_quantile")

    source_config = SourceConfig()
    reader = build_provider_batch_reader(
        source_config,
        done_table="quantile_run_status",
        done_requires_status=True,
    )
    writer = build_provider_batch_writer(source_config, ensure_tables=["quantile"])
    calculator = CalculatorFactory.create(
        "quantile",
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
        method=args.method,
    )

    id_start = args.id_start
    id_end = args.id_end
    if args.limit_id is not None:
        id_end = args.limit_id if id_end is None else min(id_end, args.limit_id)

    lake_filter = None
    if id_start > 0 or id_end is not None:
        lake_filter = RangeFilter(start=id_start, end=id_end)

    engine = Engine(
        reader=reader,
        writer=writer,
        calculator=calculator,
        algorithm="quantile",
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
