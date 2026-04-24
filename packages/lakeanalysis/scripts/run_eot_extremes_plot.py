"""Plot one-lake monthly timeline with EOT high/low anomalies from database.

Usage examples:
    uv run python scripts/run_eot_extremes_plot.py --hylak-id 1023
    uv run python scripts/run_eot_extremes_plot.py --hylak-id 1023 --threshold-quantile 0.95
    uv run python scripts/run_eot_extremes_plot.py --hylak-id 1023 --tail high
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt

from lakesource.postgres import fetch_eot_extremes_by_id, fetch_lake_area_by_ids, series_db
from lakeanalysis.eot import plot_eot_extremes_from_db
from lakeanalysis.logger import Logger
from lakeviz.plot_config import setup_chinese_font

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "eot" / "db_plot"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for EOT DB plotting."""
    parser = argparse.ArgumentParser(
        description="Plot monthly lake-area timeline with EOT anomalies from DB."
    )
    parser.add_argument(
        "--hylak-id",
        type=int,
        required=True,
        help="Target lake id.",
    )
    parser.add_argument(
        "--threshold-quantile",
        type=float,
        default=None,
        help="Optional quantile filter for eot_extremes (e.g. 0.95).",
    )
    parser.add_argument(
        "--tail",
        choices=["high", "low", "both"],
        default="both",
        help="Tail(s) to show on the timeline.",
    )
    parser.add_argument(
        "--annotate-top-n",
        type=int,
        default=8,
        help="Annotate top N strongest anomalies for each tail.",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> Path:
    """Read one lake from DB and save anomaly timeline plot."""
    with series_db.connection_context() as conn:
        series_map = fetch_lake_area_by_ids(conn, [args.hylak_id])
        series_df = series_map.get(args.hylak_id)
        if series_df is None:
            raise ValueError(f"No lake_area series found for hylak_id={args.hylak_id}")

        extremes_df = fetch_eot_extremes_by_id(
            conn,
            hylak_id=args.hylak_id,
            threshold_quantile=args.threshold_quantile,
        )

    if extremes_df.empty:
        log.warning(
            "No eot_extremes rows found for hylak_id=%d (threshold_quantile=%s)",
            args.hylak_id,
            args.threshold_quantile,
        )
    elif args.tail != "both":
        extremes_df = extremes_df[extremes_df["tail"] == args.tail].reset_index(drop=True)

    setup_chinese_font()
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fig = plot_eot_extremes_from_db(
        hylak_id=args.hylak_id,
        series_df=series_df,
        extremes_df=extremes_df,
        annotate_top_n_each_tail=args.annotate_top_n,
    )

    q_tag = (
        "allq"
        if args.threshold_quantile is None
        else f"q{args.threshold_quantile:.2f}"
    )
    out_path = DATA_DIR / f"hylak_{args.hylak_id}_{args.tail}_{q_tag}_timeline.png"
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main() -> None:
    """Entry point for CLI execution."""
    args = parse_args()
    Logger("run_eot_extremes_plot")
    out_path = run(args)
    log.info("Saved plot: %s", out_path)


if __name__ == "__main__":
    main()
