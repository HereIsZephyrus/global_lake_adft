"""Post-process comparison results: compute agreement metrics.

Reads quantile_extremes.parquet and pwm_extreme_thresholds.parquet from
the comparison output directory and computes agreement statistics.

Usage:
    python scripts/comparison_post_process.py \
        --input-dir /data/users/guxh01/2026_tcb/lake/lake_data/comparison \
        --output-dir /data/users/guxh01/2026_tcb/lake/lake_data/comparison
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Post-process comparison results.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-dir", type=str, required=True,
        help="Directory containing comparison output parquet files.",
    )
    parser.add_argument(
        "--output-dir", type=str, default=None,
        help="Output directory for agreement files. Defaults to input-dir.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = Path(args.input_dir)
    output_path = Path(args.output_dir) if args.output_dir else input_path

    if not input_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_path}")

    output_path.mkdir(parents=True, exist_ok=True)

    extremes_path = input_path / "quantile_extremes.parquet"
    thresholds_path = input_path / "pwm_extreme_thresholds.parquet"
    labels_path = input_path / "quantile_labels.parquet"

    if not extremes_path.exists():
        raise FileNotFoundError(f"Missing: {extremes_path}")
    if not thresholds_path.exists():
        raise FileNotFoundError(f"Missing: {thresholds_path}")

    log.info("Loading quantile_extremes from %s", extremes_path)
    quantile_extremes = pd.read_parquet(extremes_path)
    log.info("Loaded %d rows", len(quantile_extremes))

    log.info("Loading pwm_extreme_thresholds from %s", thresholds_path)
    pwm_thresholds = pd.read_parquet(thresholds_path)
    log.info("Loaded %d rows", len(pwm_thresholds))

    out_extremes = output_path / "comparison_extremes.parquet"
    out_thresholds = output_path / "comparison_thresholds.parquet"
    quantile_extremes.to_parquet(out_extremes, index=False)
    pwm_thresholds.to_parquet(out_thresholds, index=False)
    log.info("Wrote %s (%d rows)", out_extremes, len(quantile_extremes))
    log.info("Wrote %s (%d rows)", out_thresholds, len(pwm_thresholds))

    if labels_path.exists():
        log.info("Loading quantile_labels from %s", labels_path)
        quantile_labels = pd.read_parquet(labels_path)
        log.info("Loaded %d rows", len(quantile_labels))
        out_labels = output_path / "comparison_labels.parquet"
        quantile_labels.to_parquet(out_labels, index=False)
        log.info("Wrote %s (%d rows)", out_labels, len(quantile_labels))

    _compute_agreement(quantile_extremes, pwm_thresholds, output_path)


def _compute_agreement(
    quantile_extremes: pd.DataFrame,
    pwm_thresholds: pd.DataFrame,
    output_path: Path,
) -> None:
    if quantile_extremes.empty or pwm_thresholds.empty:
        log.info("Skipping agreement computation: empty data")
        return

    log.info("Computing agreement metrics ...")

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

    _print_summary(agreement)


def _print_summary(agreement: pd.DataFrame) -> None:
    log.info("=== Agreement Summary ===")
    log.info("Total lakes: %d", len(agreement))
    log.info("Quantile high events: %d", agreement["quantile_high_count"].sum())
    log.info("Quantile low events: %d", agreement["quantile_low_count"].sum())
    log.info("PWM high months: %d", agreement["pwm_high_months"].sum())
    log.info("PWM converged months: %d", agreement["pwm_converged_months"].sum())


if __name__ == "__main__":
    main()