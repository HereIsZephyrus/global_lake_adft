"""Run algorithm comparison (Quantile vs PWM Extreme) on all or sampled lakes.

Supports two modes:
  1. Range-based (default): processes all lakes from id_start to id_end.
  2. Sample-based (--sample-file): processes only lakes listed in a Parquet file.

Results are written via the configured backend (Parquet or PostgreSQL).
A post-processing step computes comparison metrics from the output tables.

Usage
-----
    # Full run, range-based (MPI)
    mpirun -np 128 python scripts/run_algorithm_comparison.py \
        --chunk-size 10000 --io-budget 127

    # Sample-based
    mpirun -np 10 python scripts/run_algorithm_comparison.py \
        --sample-file data/comparison/sample_lakes.parquet \
        --chunk-size 5000 --io-budget 4
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

os.environ.setdefault("NUMBA_NUM_THREADS", "1")

import pandas as pd

from lakesource.config import Backend, SourceConfig
from lakesource.provider import create_provider

from lakeanalysis.batch import Engine, IdSetFilter, RangeFilter
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run algorithm comparison (Quantile vs PWM Extreme).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sample-file", type=str, default=None,
        help="Path to sample_lakes.parquet. If omitted, runs range-based on all lakes.",
    )
    parser.add_argument("--chunk-size", type=int, default=10_000)
    parser.add_argument("--limit-id", type=int, default=None)
    parser.add_argument("--id-start", type=int, default=0)
    parser.add_argument("--id-end", type=int, default=None)
    parser.add_argument("--io-budget", type=int, default=4)
    parser.add_argument("--min-valid-per-month", type=int, default=None)
    parser.add_argument("--min-valid-observations", type=int, default=None)
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Directory for post-processing Parquet output. Defaults to data_dir when using Parquet backend.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_algorithm_comparison")

    config = SourceConfig()
    provider = create_provider(config)
    calculator = CalculatorFactory.create(
        "comparison",
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
    )

    lake_filter = None
    sample_ids = None

    if args.sample_file:
        sample_path = Path(args.sample_file)
        if not sample_path.exists():
            raise FileNotFoundError(f"Sample file not found: {sample_path}")
        log.info("Loading sample from %s", sample_path)
        sample_df = pd.read_parquet(sample_path)
        if "hylak_id" not in sample_df.columns:
            raise ValueError("Sample file must contain 'hylak_id' column")
        sample_ids = set(sample_df["hylak_id"].astype(int))
        log.info("Loaded %d lake IDs from sample", len(sample_ids))
        lake_filter = IdSetFilter(sample_ids)
    else:
        id_start = args.id_start
        id_end = args.id_end
        if args.limit_id is not None:
            id_end = args.limit_id if id_end is None else min(id_end, args.limit_id)
        if id_start > 0 or id_end is not None:
            lake_filter = RangeFilter(start=id_start, end=id_end)

    engine = Engine(
        provider=provider,
        calculator=calculator,
        algorithm="comparison",
        lake_filter=lake_filter,
        chunk_size=args.chunk_size,
        io_budget=args.io_budget,
    )

    report = engine.run()
    if report:
        log.info(
            "Done: chunks=%d/%d success=%d error=%d skipped=%d",
            report.processed_chunks,
            report.total_chunks,
            report.success_lakes,
            report.error_lakes,
            report.skipped_lakes,
        )

    output_dir = args.output_dir
    if output_dir is None and config.backend == Backend.PARQUET:
        output_dir = str(config.output_dir or config.data_dir or "")
    if output_dir:
        _post_process(config, output_dir, sample_ids)


def _post_process(
    config: SourceConfig, output_dir: str, sample_ids: set[int] | None
) -> None:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if config.backend == Backend.PARQUET:
        _post_process_parquet(config, output_path, sample_ids)
    else:
        _post_process_postgres(config, output_path, sample_ids)


def _post_process_parquet(
    config: SourceConfig, output_path: Path, sample_ids: set[int] | None
) -> None:
    data_dir = config.output_dir or config.data_dir
    if data_dir is None:
        log.warning("No output_dir or data_dir configured, skipping post-processing")
        return

    log.info("Post-processing comparison metrics from Parquet ...")

    extremes_path = data_dir / "quantile_extremes.parquet"
    thresholds_path = data_dir / "pwm_extreme_thresholds.parquet"
    labels_path = data_dir / "quantile_labels.parquet"

    if not extremes_path.exists() or not thresholds_path.exists():
        log.warning("Missing output parquet files, skipping agreement computation")
        return

    quantile_extremes = pd.read_parquet(extremes_path)
    pwm_thresholds = pd.read_parquet(thresholds_path)

    if sample_ids is not None:
        quantile_extremes = quantile_extremes[
            quantile_extremes["hylak_id"].isin(sample_ids)
        ]
        pwm_thresholds = pwm_thresholds[
            pwm_thresholds["hylak_id"].isin(sample_ids)
        ]

    out_extremes = output_path / "comparison_extremes.parquet"
    out_thresholds = output_path / "comparison_thresholds.parquet"
    quantile_extremes.to_parquet(out_extremes, index=False)
    pwm_thresholds.to_parquet(out_thresholds, index=False)
    log.info("Wrote %s (%d rows)", out_extremes, len(quantile_extremes))
    log.info("Wrote %s (%d rows)", out_thresholds, len(pwm_thresholds))

    if labels_path.exists():
        quantile_labels = pd.read_parquet(labels_path)
        if sample_ids is not None:
            quantile_labels = quantile_labels[
                quantile_labels["hylak_id"].isin(sample_ids)
            ]
        out_labels = output_path / "comparison_labels.parquet"
        quantile_labels.to_parquet(out_labels, index=False)
        log.info("Wrote %s (%d rows)", out_labels, len(quantile_labels))

    _compute_agreement(quantile_extremes, pwm_thresholds, output_path)


def _post_process_postgres(
    config: SourceConfig, output_path: Path, sample_ids: set[int] | None
) -> None:
    from lakesource.postgres import series_db

    if sample_ids is None:
        log.warning(
            "PostgreSQL post-processing requires sample_ids; "
            "skipping for full range-based run"
        )
        return

    ids = sorted(sample_ids)
    if not ids:
        return

    log.info("Post-processing comparison metrics for %d lakes ...", len(ids))

    with series_db.connection_context() as conn:
        quantile_extremes = pd.read_sql(
            """
            SELECT hylak_id, year, month, event_type, water_area, threshold
            FROM quantile_extremes
            WHERE hylak_id = ANY(%(ids)s)
            ORDER BY hylak_id, year, month
            """,
            conn,
            params={"ids": ids},
        )

        pwm_thresholds = pd.read_sql(
            """
            SELECT hylak_id, month, threshold_high, threshold_low, converged
            FROM pwm_extreme_thresholds
            WHERE hylak_id = ANY(%(ids)s)
            ORDER BY hylak_id, month
            """,
            conn,
            params={"ids": ids},
        )

        quantile_labels = pd.read_sql(
            """
            SELECT hylak_id, year, month, anomaly, q_low, q_high, extreme_label
            FROM quantile_labels
            WHERE hylak_id = ANY(%(ids)s)
              AND extreme_label != 'normal'
            ORDER BY hylak_id, year, month
            """,
            conn,
            params={"ids": ids},
        )

    extremes_path = output_path / "comparison_extremes.parquet"
    thresholds_path = output_path / "comparison_thresholds.parquet"
    labels_path = output_path / "comparison_labels.parquet"

    quantile_extremes.to_parquet(extremes_path, index=False)
    pwm_thresholds.to_parquet(thresholds_path, index=False)
    quantile_labels.to_parquet(labels_path, index=False)

    log.info("Wrote %s (%d rows)", extremes_path, len(quantile_extremes))
    log.info("Wrote %s (%d rows)", thresholds_path, len(pwm_thresholds))
    log.info("Wrote %s (%d rows)", labels_path, len(quantile_labels))

    _compute_agreement(quantile_extremes, pwm_thresholds, output_path)


def _compute_agreement(
    quantile_extremes: pd.DataFrame,
    pwm_thresholds: pd.DataFrame,
    output_path: Path,
) -> None:
    if quantile_extremes.empty or pwm_thresholds.empty:
        log.info("Skipping agreement computation: empty data")
        return

    q_high = quantile_extremes[quantile_extremes["event_type"] == "high"]
    q_low = quantile_extremes[quantile_extremes["event_type"] == "low"]

    q_high_counts = q_high.groupby("hylak_id").size().rename("quantile_high_count")
    q_low_counts = q_low.groupby("hylak_id").size().rename("quantile_low_count")

    pwm_months = pwm_thresholds.groupby("hylak_id").agg(
        pwm_high_months=("threshold_high", "count"),
        pwm_converged_months=("converged", "sum"),
    )

    agreement = pd.concat([q_high_counts, q_low_counts, pwm_months], axis=1).fillna(0).astype(int)
    agreement = agreement.reset_index()

    agreement_path = output_path / "comparison_agreement.parquet"
    agreement.to_parquet(agreement_path, index=False)
    log.info("Wrote %s (%d rows)", agreement_path, len(agreement))


if __name__ == "__main__":
    main()
