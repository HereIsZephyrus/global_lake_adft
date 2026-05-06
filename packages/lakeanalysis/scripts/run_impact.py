"""Compute human-impact metrics for artificial-natural lake pairs (topo_level>8).

Steps:
  1. Fetch af_nearest (topo_level > 8) and lake_area for all involved hylak_ids.
  2. Filter out pairs where either lake appears in area_anomalies.
  3. For each pair: compute volatility metrics (CV, pct_change_std, range_ratio)
     and Z-score anomaly event statistics.
  4. Write results to data/impact/impact.csv.
  5. Optionally plot distributions and typical cases (--plot / --plot-only).

Usage:
  uv run python scripts/run_impact.py
  uv run python scripts/run_impact.py --limit-pairs 500
  uv run python scripts/run_impact.py --z-threshold 2.5
  uv run python scripts/run_impact.py --plot
  uv run python scripts/run_impact.py --plot-only
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
from lakeanalysis.artificial.impact.events import compute_pair_events
from lakeanalysis.artificial.fetch import load_pairs_and_areas
from lakeanalysis.artificial.impact.metrics import compute_pair_metrics
from lakeviz.plot_config import setup_chinese_font
from lakeviz.artificial import (
    plot_anomaly_ratio_comparison,
    plot_delta_cv_distribution,
    plot_typical_pair_timeline,
    plot_volatility_comparison,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "impact"
IMPACT_CSV = DATA_DIR / "impact.csv"


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for run_impact."""
    parser = argparse.ArgumentParser(
        description="Compute human-impact metrics for artificial-natural lake pairs."
    )
    parser.add_argument(
        "--limit-pairs",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N pairs (for testing).",
    )
    parser.add_argument(
        "--z-threshold",
        type=float,
        default=3.0,
        metavar="Z",
        help="Z-score threshold for anomaly event detection (default: 3.0).",
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate matplotlib plots after computation.",
    )
    parser.add_argument(
        "--plot-only",
        action="store_true",
        help="Load from impact.csv and plot only; skip computation.",
    )
    return parser.parse_args()


def run(
    limit_pairs: int | None = None,
    z_threshold: float = 3.0,
    show_plot: bool = False,
) -> None:
    """Load pairs and lake_area, compute impact metrics, write CSV and optionally plot."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    with series_db.connection_context() as conn:
        pairs, lake_frames = load_pairs_and_areas(conn)

    if not pairs:
        log.warning("No af_nearest pairs with topo_level>8 found after quality filtering.")
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
        df_n = lake_frames.get(nearest_id)
        if df_a is None or df_n is None:
            log.debug("Skip pair (%d, %d): missing lake_area", hylak_id, nearest_id)
            continue

        vol = compute_pair_metrics(df_a, df_n)
        evt = compute_pair_events(df_a, df_n, threshold=z_threshold)

        rows.append({
            "hylak_id": hylak_id,
            "nearest_id": nearest_id,
            "topo_level": topo_level,
            "cv_a": vol["cv_a"],
            "cv_n": vol["cv_n"],
            "delta_cv": vol["delta_cv"],
            "pct_change_std_a": vol["pct_change_std_a"],
            "pct_change_std_n": vol["pct_change_std_n"],
            "delta_pct_change_std": vol["delta_pct_change_std"],
            "range_ratio_a": vol["range_ratio_a"],
            "range_ratio_n": vol["range_ratio_n"],
            "delta_range_ratio": vol["delta_range_ratio"],
            "n_obs_a": vol["n_obs_a"],
            "n_obs_n": vol["n_obs_n"],
            "n_events_a": evt["n_events_a"],
            "n_events_n": evt["n_events_n"],
            "anomaly_ratio_a": evt["anomaly_ratio_a"],
            "anomaly_ratio_n": evt["anomaly_ratio_n"],
            "delta_anomaly_ratio": evt["delta_anomaly_ratio"],
            "n_unique_a": evt["n_unique_a"],
            "z_threshold": z_threshold,
        })

    if not rows:
        log.warning("No pairs with valid lake_area data.")
        return

    _write_csv(rows)
    log.info("Wrote %d rows to %s", len(rows), IMPACT_CSV)

    if show_plot:
        _show_plots(rows, lake_frames)


_FIELDNAMES = [
    "hylak_id", "nearest_id", "topo_level",
    "cv_a", "pct_change_std_a", "range_ratio_a", "n_obs_a",
    "cv_n", "pct_change_std_n", "range_ratio_n", "n_obs_n",
    "delta_cv", "delta_pct_change_std", "delta_range_ratio",
    "n_events_a", "anomaly_ratio_a",
    "n_events_n", "anomaly_ratio_n",
    "n_unique_a", "delta_anomaly_ratio",
]


def _write_csv(rows: list[dict]) -> None:
    fieldnames = _FIELDNAMES
    with open(IMPACT_CSV, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _load_impact_csv() -> pd.DataFrame:
    if not IMPACT_CSV.exists():
        log.warning("CSV not found: %s", IMPACT_CSV)
        return pd.DataFrame()
    return pd.read_csv(IMPACT_CSV)


def _show_plots(
    rows: list[dict],
    lake_frames: dict[int, pd.DataFrame],
) -> None:
    impact_df = pd.DataFrame(rows)
    if impact_df.empty:
        log.warning("No data for plotting.")
        return

    log.info("Plotting impact summary for %d pairs.", len(impact_df))
    setup_chinese_font()
    plot_dir = DATA_DIR / "plot"
    plot_dir.mkdir(parents=True, exist_ok=True)

    fig = plot_volatility_comparison(impact_df)
    fig.savefig(plot_dir / "volatility_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_delta_cv_distribution(impact_df)
    fig.savefig(plot_dir / "delta_cv_distribution.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    fig = plot_anomaly_ratio_comparison(impact_df)
    fig.savefig(plot_dir / "anomaly_ratio_comparison.png", dpi=300, bbox_inches="tight")
    plt.close(fig)

    top_pairs = impact_df.nlargest(3, "delta_cv")
    for _, pair in top_pairs.iterrows():
        hylak_id = int(pair["hylak_id"])
        nearest_id = int(pair["nearest_id"])
        df_a = lake_frames.get(hylak_id)
        df_n = lake_frames.get(nearest_id)
        if df_a is None or df_n is None:
            continue
        fig = plot_typical_pair_timeline(df_a, df_n, pair.to_dict())
        fig.savefig(
            plot_dir / f"timeline_{hylak_id}_{nearest_id}.png",
            dpi=300, bbox_inches="tight",
        )
        plt.close(fig)

    log.info("Saved plots to %s", plot_dir)


def main() -> None:
    """Entry point: parse args, run pipeline or plot-only."""
    args = parse_args()
    Logger("run_impact")
    if args.plot_only:
        impact_df = _load_impact_csv()
        if impact_df.empty:
            return
        setup_chinese_font()
        plot_dir = DATA_DIR / "plot"
        plot_dir.mkdir(parents=True, exist_ok=True)

        fig = plot_volatility_comparison(impact_df)
        fig.savefig(plot_dir / "volatility_comparison.png", dpi=300, bbox_inches="tight")
        plt.close(fig)

        fig = plot_delta_cv_distribution(impact_df)
        fig.savefig(plot_dir / "delta_cv_distribution.png", dpi=300, bbox_inches="tight")
        plt.close(fig)

        fig = plot_anomaly_ratio_comparison(impact_df)
        fig.savefig(plot_dir / "anomaly_ratio_comparison.png", dpi=300, bbox_inches="tight")
        plt.close(fig)
    else:
        run(limit_pairs=args.limit_pairs, z_threshold=args.z_threshold, show_plot=args.plot)


if __name__ == "__main__":
    main()
