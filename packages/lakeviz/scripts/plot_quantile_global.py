"""Generate global distribution maps for quantile-based identification results.

Usage:
    uv run python scripts/plot_quantile_global.py
    uv run python scripts/plot_quantile_global.py --refresh
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakeviz.config import GlobalGridConfig
from lakeviz.plot_config import setup_chinese_font
from lakeviz.quantile import (
    plot_extremes_by_type_map,
    plot_extremes_density_map,
    plot_transition_by_type_map,
    plot_transition_density_map,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate global quantile distribution maps.")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures")
    parser.add_argument("--resolution", type=float, default=0.5)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    load_env()
    setup_chinese_font()

    source = SourceConfig()
    grid_config = GlobalGridConfig(source=source, resolution=args.resolution, output_dir=args.output_dir)

    plot_fns = [
        ("extremes_density", lambda: plot_extremes_density_map(grid_config, refresh=args.refresh)),
        ("extremes_high", lambda: plot_extremes_by_type_map(grid_config, "high", refresh=args.refresh)),
        ("extremes_low", lambda: plot_extremes_by_type_map(grid_config, "low", refresh=args.refresh)),
        ("transition_density", lambda: plot_transition_density_map(grid_config, refresh=args.refresh)),
        ("transition_low_to_high", lambda: plot_transition_by_type_map(grid_config, "low_to_high", refresh=args.refresh)),
        ("transition_high_to_low", lambda: plot_transition_by_type_map(grid_config, "high_to_low", refresh=args.refresh)),
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
