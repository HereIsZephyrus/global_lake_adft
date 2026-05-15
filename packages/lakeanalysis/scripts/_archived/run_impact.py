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
from pathlib import Path

from lakeanalysis.artificial.impact.runner import (
    ImpactRunConfig,
    load_impact_csv,
    run_impact,
    show_impact_plots,
)
from lakeanalysis.logger import Logger

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "impact"


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


def main() -> None:
    """Entry point: parse args, run pipeline or plot-only."""
    args = parse_args()
    Logger("run_impact")
    if args.plot_only:
        impact_df = load_impact_csv(DATA_DIR)
        if impact_df.empty:
            return
        show_impact_plots(DATA_DIR, rows=impact_df.to_dict("records"))
    else:
        run_impact(
            ImpactRunConfig(
                data_dir=DATA_DIR,
                limit_pairs=args.limit_pairs,
                z_threshold=args.z_threshold,
                show_plot=args.plot,
            )
        )


if __name__ == "__main__":
    main()
