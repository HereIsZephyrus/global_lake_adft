"""Run area comparison analysis: rs_area vs atlas_area from area_quality.

Reads area_quality data via DuckDB/Parquet, computes comparison metrics,
outputs summary statistics, CSV report, and visualization plots.

Usage:
    uv run python scripts/run_area_comparison.py
    uv run python scripts/run_area_comparison.py --data-dir /path/to/parquet
    uv run python scripts/run_area_comparison.py --output-dir results/comparison
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.parquet.client import DuckDBClient
from lakeanalysis.logger import Logger
from lakeanalysis.quality.comparison import (
    AgreementConfig,
    enrich_comparison_df,
    summarize_comparison,
)
from lakeviz.quality import plot_area_scatter, plot_ratio_histogram
from lakeviz.layout import save

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare rs_area vs atlas_area from area_quality.")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        metavar="DIR",
        help="Path to parquet data directory (default: from DATA_DIR env).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="results/area_comparison",
        metavar="DIR",
        help="Output directory for CSV and plots (default: results/area_comparison).",
    )
    parser.add_argument(
        "--excellent-threshold",
        type=float,
        default=0.1,
        metavar="T",
        help="Excellent agreement: ratio within ±T (default: 0.1 = ±10%%).",
    )
    parser.add_argument(
        "--good-threshold",
        type=float,
        default=2.0,
        metavar="G",
        help="Good agreement: ratio within [1/G, G] (default: 2.0).",
    )
    parser.add_argument(
        "--moderate-threshold",
        type=float,
        default=5.0,
        metavar="M",
        help="Moderate agreement: ratio within [1/M, M] (default: 5.0).",
    )
    parser.add_argument(
        "--poor-threshold",
        type=float,
        default=10.0,
        metavar="P",
        help="Poor agreement: ratio within [1/P, P] (default: 10.0).",
    )
    return parser.parse_args()


def load_area_quality(data_dir: Path) -> pd.DataFrame:
    """Load area_quality table via DuckDB from parquet files."""
    with DuckDBClient(data_dir=data_dir) as client:
        tables = client.list_registered_tables()
        if "area_quality" not in tables:
            raise FileNotFoundError(
                f"area_quality not found in {data_dir}. Available: {tables}"
            )
        df = client.query_df(
            "SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area, computed_at "
            "FROM area_quality "
            "ORDER BY hylak_id"
        )
    log.info("Loaded %d rows from area_quality", len(df))
    return df


def print_summary(summary: dict) -> None:
    """Print summary statistics to log."""
    n = summary["n_total"]
    if n == 0:
        log.warning("No valid data for comparison (atlas_area > 0)")
        return

    log.info("=" * 60)
    log.info("Area Comparison Summary (n = %d)", n)
    log.info("=" * 60)

    counts = summary["n_by_agreement"]
    for level in ["excellent", "good", "moderate", "poor", "extreme"]:
        c = counts.get(level, 0)
        pct = c / n * 100
        log.info("  %-12s: %8d  (%5.1f%%)", level, c, pct)

    log.info("-" * 60)
    log.info("  median_ratio     : %.4f", summary["median_ratio"])
    log.info("  mean_log2_ratio  : %.4f", summary["mean_log2_ratio"])
    log.info("  iqr_ratio        : %.4f", summary["iqr_ratio"])
    log.info("  std_log2_ratio   : %.4f", summary["std_log2_ratio"])
    log.info("  p05 / p25 / p50 / p75 / p95 ratio:")
    log.info("    %.4f / %.4f / %.4f / %.4f / %.4f",
             summary["p05_ratio"], summary["p25_ratio"],
             summary["p50_ratio"], summary["p75_ratio"],
             summary["p95_ratio"])
    log.info("-" * 60)
    log.info("  Overestimate  (ratio > 1+T): %d (%.1f%%)",
             summary["n_overestimate"], summary["n_overestimate"] / n * 100)
    log.info("  Underestimate (ratio < 1-T): %d (%.1f%%)",
             summary["n_underestimate"], summary["n_underestimate"] / n * 100)
    log.info("  Agree (±T)               : %d (%.1f%%)",
             summary["n_agree"], summary["n_agree"] / n * 100)
    log.info("=" * 60)


def run(args: argparse.Namespace) -> None:
    data_dir = Path(args.data_dir) if args.data_dir else Path(
        __import__("os").environ.get("DATA_DIR", "data/parquet")
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    config = AgreementConfig(
        excellent=args.excellent_threshold,
        good=args.good_threshold,
        moderate=args.moderate_threshold,
        poor=args.poor_threshold,
    )
    log.info("AgreementConfig: excellent=±%.2f, good=%.1fx, moderate=%.1fx, poor=%.1fx",
             config.excellent, config.good, config.moderate, config.poor)

    df = load_area_quality(data_dir)
    if df.empty:
        log.warning("area_quality table is empty, nothing to compare")
        return

    enriched = enrich_comparison_df(df, config=config)

    summary = summarize_comparison(enriched, config=config)
    print_summary(summary)

    csv_path = output_dir / "area_comparison.csv"
    enriched.to_csv(csv_path, index=False)
    log.info("Saved enriched CSV to %s", csv_path)

    fig_scatter = plot_area_scatter(enriched)
    save(fig_scatter, output_dir / "area_scatter.png")

    fig_hist = plot_ratio_histogram(enriched)
    save(fig_hist, output_dir / "ratio_histogram.png")

    log.info("Done. Results in %s", output_dir)


def main() -> None:
    args = parse_args()
    Logger("run_area_comparison")
    run(args)


if __name__ == "__main__":
    main()
