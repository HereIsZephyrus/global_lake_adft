"""Compute lake-pair similarity (Pearson + ACF cosine) for af_nearest pairs with topo_level>8.

Steps:
  1. Fetch af_nearest (topo_level > 8) and lake_area for all involved hylak_ids from SERIES_DB.
  2. For each pair (hylak_id, nearest_id), align water_area series and compute pearson_r,
     acf_cos_sim (12-month delay), and n_common.
  3. Write results to data/similarity/similarity.csv.
  4. Optionally plot distributions and scatter (--plot / --plot-only).

Usage:
  uv run python scripts/run_similarity.py
  uv run python scripts/run_similarity.py --limit-pairs 500
  uv run python scripts/run_similarity.py --plot
  uv run python scripts/run_similarity.py --plot-only
"""

from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from lakesource.postgres import series_db
from lakeanalysis.logger import Logger
from lakeanalysis.similarity.compute import compute_pair_similarity
from lakeanalysis.similarity.fetch import load_pairs_and_areas
from lakeanalysis.plot_config import setup_chinese_font
from lakeanalysis.similarity.plot import (
    plot_acf_cosine_distribution,
    plot_pearson_distribution,
    plot_pearson_vs_acf,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "similarity"
SIMILARITY_CSV = DATA_DIR / "similarity.csv"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for run_similarity."""
    parser = argparse.ArgumentParser(
        description="Compute Pearson and ACF cosine similarity for lake pairs (topo_level>8)."
    )
    parser.add_argument(
        "--limit-pairs",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N pairs (for testing).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate matplotlib plots after computation.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Load from similarity.csv and plot only; skip computation.",
    )
    return parser.parse_args()


def run(limit_pairs: int | None = None, show_plot: bool = False) -> None:
    """Load pairs and lake_area, compute similarity, write CSV and optionally plot."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        pairs, lake_frames = load_pairs_and_areas(conn)

    if not pairs:
        log.warning("No af_nearest pairs with topo_level>8 found.")
        return

    if limit_pairs is not None:
        pairs = pairs[:limit_pairs]
        log.info("Limited to first %d pairs", len(pairs))

    rows = []
    for rec in pairs:
        hylak_id = rec["hylak_id"]
        nearest_id = rec["nearest_id"]
        topo_level = rec["topo_level"]
        df_a = lake_frames.get(hylak_id)
        df_b = lake_frames.get(nearest_id)
        if df_a is None or df_b is None:
            log.debug("Skip pair (%d, %d): missing lake_area", hylak_id, nearest_id)
            continue
        metrics = compute_pair_similarity(df_a, df_b)
        rows.append({
            "hylak_id": hylak_id,
            "nearest_id": nearest_id,
            "topo_level": topo_level,
            "pearson_r": metrics["pearson_r"],
            "acf_cos_sim": metrics["acf_cos_sim"],
            "n_common": metrics["n_common"],
        })

    if not rows:
        log.warning("No pairs with valid lake_area data.")
        return

    _write_csv(rows)
    log.info("Wrote %d rows to %s", len(rows), SIMILARITY_CSV)

    if show_plot:
        _show_plots()


def _write_csv(rows: list[dict]) -> None:
    """Write similarity results to CSV."""
    fieldnames = ["hylak_id", "nearest_id", "topo_level", "pearson_r", "acf_cos_sim", "n_common"]
    with open(SIMILARITY_CSV, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_summary_csv() -> pd.DataFrame:
    """Load similarity summary from data/similarity/similarity.csv."""
    if not SIMILARITY_CSV.exists():
        log.warning("CSV not found: %s", SIMILARITY_CSV)
        return pd.DataFrame()
    return pd.read_csv(SIMILARITY_CSV)


def _show_plots() -> None:
    """Load CSV and save plots to data/similarity/plot/."""
    summary_df = _load_summary_csv()
    if summary_df.empty:
        log.warning("No data for plotting.")
        return

    log.info("Plotting summary for %d pairs.", len(summary_df))
    setup_chinese_font()
    plot_dir = DATA_DIR / "plot"
    plot_dir.mkdir(parents=True, exist_ok=True)

    fig = plot_pearson_distribution(summary_df)
    fig.savefig(plot_dir / "pearson_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_acf_cosine_distribution(summary_df)
    fig.savefig(plot_dir / "acf_cosine_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_pearson_vs_acf(summary_df)
    fig.savefig(plot_dir / "pearson_vs_acf.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    log.info("Saved plots to %s", plot_dir)


def main() -> None:
    """Entry point: parse args, run pipeline or plot-only."""
    args = parse_args()
    Logger("run_similarity")
    if args.plot_only:
        _show_plots()
    else:
        run(limit_pairs=args.limit_pairs, show_plot=args.plot)


if __name__ == "__main__":
    main()
