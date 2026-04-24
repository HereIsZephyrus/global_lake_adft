"""Generate global distribution maps for monthly transition (quantile-based) results.

Steps:
  1. Load extremes and transitions data with lake coordinates (with parquet cache).
  2. Generate global maps for extremes density, transitions density, and by-type breakdowns.
  3. Save figures to figures/monthly_transition/.

Usage:
    uv run python scripts/plot_monthly_transition_global.py
    uv run python scripts/plot_monthly_transition_global.py --refresh
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakeviz.config import GlobalGridConfig
from lakeviz.monthly_transition import (
    plot_extremes_by_type_map,
    plot_extremes_density_map,
    plot_transition_by_type_map,
    plot_transition_density_map,
)
from lakeviz.plot_config import setup_chinese_font

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate global monthly transition distribution maps.",
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

    plot_fns = [
        ("extremes_density", lambda: plot_extremes_density_map(grid_config, refresh=args.refresh)),
        ("extremes_dry", lambda: plot_extremes_by_type_map(grid_config, "dry", refresh=args.refresh)),
        ("extremes_wet", lambda: plot_extremes_by_type_map(grid_config, "wet", refresh=args.refresh)),
        ("transition_density", lambda: plot_transition_density_map(grid_config, refresh=args.refresh)),
        ("transition_dry_to_wet", lambda: plot_transition_by_type_map(grid_config, "dry_to_wet", refresh=args.refresh)),
        ("transition_wet_to_dry", lambda: plot_transition_by_type_map(grid_config, "wet_to_dry", refresh=args.refresh)),
    ]

    for name, fn in plot_fns:
        try:
            out = fn()
            if out and Path(out).exists():
                log.info("Saved: %s → %s", name, out)
            else:
                log.warning("Skipped: %s (no data)", name)
        except Exception:
            log.exception("Failed: %s", name)
        plt.close("all")


if __name__ == "__main__":
    main()
