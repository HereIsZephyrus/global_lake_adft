"""Run algorithm comparison (Quantile vs PWM Extreme) on sampled lakes.

Reads a sample Parquet file produced by sample_test_lakes.py, creates
an IdSetFilter from the hylak_id column, and runs both algorithms
through the unified batch framework.  Results are written to
PostgreSQL; a post-processing step then computes comparison metrics
and writes them to Parquet.

Usage
-----
    # Single-process
    python scripts/run_algorithm_comparison.py \
        --sample-file data/comparison/sample_lakes.parquet

    # MPI with 10 cores
    mpiexec -np 10 python scripts/run_algorithm_comparison.py \
        --sample-file data/comparison/sample_lakes.parquet \
        --chunk-size 5000 --io-budget 4
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.provider import create_provider

from lakeanalysis.batch import Engine, IdSetFilter
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run algorithm comparison on sampled lakes.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sample-file", type=str, required=True,
        help="Path to sample_lakes.parquet from sample_test_lakes.py.",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=5_000,
        help="Number of lake IDs per batch.",
    )
    parser.add_argument(
        "--io-budget", type=int, default=4,
        help="Max concurrent DB IO workers.",
    )
    parser.add_argument(
        "--min-valid-per-month", type=int, default=None,
    )
    parser.add_argument(
        "--min-valid-observations", type=int, default=None,
    )
    parser.add_argument(
        "--output-dir", type=str, default="data/comparison",
        help="Directory for post-processing Parquet output.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    Logger("run_algorithm_comparison")

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
    provider = create_provider(SourceConfig())
    calculator = CalculatorFactory.create(
        "comparison",
        min_valid_per_month=args.min_valid_per_month,
        min_valid_observations=args.min_valid_observations,
    )

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

    _post_process(sample_df, args.output_dir)


def _post_process(sample_df: pd.DataFrame, output_dir: str) -> None:
    from lakesource.postgres import series_db

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    ids = sample_df["hylak_id"].astype(int).tolist()
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