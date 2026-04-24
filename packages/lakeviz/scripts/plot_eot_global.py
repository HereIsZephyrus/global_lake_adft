"""Generate global distribution maps for EOT results.

Steps:
  1. Query available (tail, threshold_quantile) combinations from eot_results.
  2. For each combination, generate 5 global maps:
     - convergence_rate, median_xi, median_sigma, extremes_frequency, median_threshold.
  3. Save figures to figures/eot/{tail}/q{quantile}/.

Usage:
    uv run python scripts/plot_eot_global.py
    uv run python scripts/plot_eot_global.py --tail high --quantile 0.95
    uv run python scripts/plot_eot_global.py --refresh
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt

from lakesource.config import SourceConfig
from lakesource.eot.reader import fetch_available_quantiles
from lakesource.env import load_env
from lakeviz.config import GlobalGridConfig
from lakeviz.eot import (
    plot_eot_convergence_map,
    plot_eot_extremes_frequency_map,
    plot_eot_sigma_map,
    plot_eot_threshold_map,
    plot_eot_xi_map,
)
from lakeviz.plot_config import setup_chinese_font

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate global EOT distribution maps.",
    )
    parser.add_argument(
        "--tail",
        choices=["high", "low"],
        default=None,
        help="Filter to a single tail direction (default: all).",
    )
    parser.add_argument(
        "--quantile",
        type=float,
        default=None,
        help="Filter to a single threshold_quantile (default: all).",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-fetch from database, overwriting parquet cache.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("figures"),
        help="Output directory for figures (default: figures/).",
    )
    parser.add_argument(
        "--resolution",
        type=float,
        default=0.5,
        help="Grid cell size in degrees (default: 0.5).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    load_env()

    setup_chinese_font()

    source = SourceConfig()
    grid_config = GlobalGridConfig(
        source=source,
        resolution=args.resolution,
        output_dir=args.output_dir,
    )

    combos = fetch_available_quantiles(source)
    if combos.empty:
        log.error("No EOT results found in database")
        return

    if args.tail is not None:
        combos = combos[combos["tail"] == args.tail]
    if args.quantile is not None:
        combos = combos[combos["threshold_quantile"].astype(float) == args.quantile]

    plot_fns = [
        ("convergence_rate", plot_eot_convergence_map),
        ("median_xi", plot_eot_xi_map),
        ("median_sigma", plot_eot_sigma_map),
        ("extremes_frequency", plot_eot_extremes_frequency_map),
        ("median_threshold", plot_eot_threshold_map),
    ]

    for _, row in combos.iterrows():
        tail = row["tail"]
        q = float(row["threshold_quantile"])
        log.info("=== EOT global maps: tail=%s, q=%.4f ===", tail, q)

        for name, fn in plot_fns:
            try:
                out = fn(grid_config, tail, q, refresh=args.refresh)
                if out and Path(out).exists():
                    log.info("  Saved: %s → %s", name, out)
                else:
                    log.warning("  Skipped: %s (no data)", name)
            except Exception:
                log.exception("  Failed: %s", name)
            plt.close("all")


if __name__ == "__main__":
    main()
