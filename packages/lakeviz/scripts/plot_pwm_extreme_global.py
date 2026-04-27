"""Generate global distribution maps for PWM extreme quantile results.

Usage:
    uv run python scripts/plot_pwm_extreme_global.py
    uv run python scripts/plot_pwm_extreme_global.py --refresh
    uv run python scripts/plot_pwm_extreme_global.py --monthly-only
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
from lakeviz.pwm_extreme import (
    plot_pwm_convergence_map,
    plot_pwm_monthly_threshold_maps,
    plot_pwm_threshold_high_map,
    plot_pwm_threshold_low_map,
)

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate global PWM extreme distribution maps.")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--monthly-only", action="store_true", help="Only generate monthly maps")
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

    if not args.monthly_only:
        plot_fns = [
            ("convergence_rate", lambda: plot_pwm_convergence_map(grid_config, refresh=args.refresh)),
            ("threshold_high", lambda: plot_pwm_threshold_high_map(grid_config, refresh=args.refresh)),
            ("threshold_low", lambda: plot_pwm_threshold_low_map(grid_config, refresh=args.refresh)),
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

    try:
        paths = plot_pwm_monthly_threshold_maps(grid_config, refresh=args.refresh)
        if paths:
            log.info("Saved %d monthly threshold maps", len(paths))
        else:
            log.warning("Skipped: monthly threshold maps (no data)")
    except Exception:
        log.exception("Failed: monthly threshold maps")
    plt.close("all")


if __name__ == "__main__":
    main()
