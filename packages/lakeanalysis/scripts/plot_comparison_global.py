"""Generate comparison density panels under data/figures/comparison/."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakesource.provider import create_provider
from lakeviz.comparison import (
    plot_eot_quantile_panels,
    plot_gt10_vs_full_panels,
    plot_pwm_pvalue_panels,
    plot_pwm_vs_eot_panels,
    plot_quantile_vs_pwm_panels,
)
from lakeviz.config import GlobalGridConfig
from lakeviz.style.presets import Theme
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot comparison density panels.")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures")
    parser.add_argument("--resolution", type=float, default=0.5)
    parser.add_argument("--pwm-p1", type=float, default=0.01)
    parser.add_argument("--pwm-p2", type=float, default=0.05)
    parser.add_argument("--eot-q1", type=float, default=0.95)
    parser.add_argument("--eot-q2", type=float, default=0.98)
    parser.add_argument("--gt10-dir", type=Path, default=DATA_DIR / "parquet_gt10")
    parser.add_argument("--full-dir", type=Path, default=DATA_DIR / "parquet")
    parser.add_argument("--hatch", action="store_true", default=False)
    return parser.parse_args()


def _log_outputs(label: str, paths: list[Path]) -> None:
    if not paths:
        log.warning("No outputs generated for %s", label)
        return
    log.info("Generated %d panels for %s", len(paths), label)
    for path in paths:
        log.info("  %s", path)


def main() -> None:
    Logger("plot_comparison_global")
    args = parse_args()
    load_env()
    Theme.apply()

    source = SourceConfig()
    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=args.resolution, output_dir=args.output_dir)

    _log_outputs(
        "pwm_pvalue",
        plot_pwm_pvalue_panels(
            grid_config,
            p1=args.pwm_p1,
            p2=args.pwm_p2,
            refresh=args.refresh,
            draw_hatch=args.hatch,
        ),
    )
    _log_outputs(
        "eot_quantile",
        plot_eot_quantile_panels(
            grid_config,
            q1=args.eot_q1,
            q2=args.eot_q2,
            refresh=args.refresh,
            draw_hatch=args.hatch,
        ),
    )
    _log_outputs(
        "quantile_vs_pwm",
        plot_quantile_vs_pwm_panels(grid_config, refresh=args.refresh, draw_hatch=args.hatch),
    )
    _log_outputs(
        "pwm_vs_eot",
        plot_pwm_vs_eot_panels(grid_config, refresh=args.refresh, draw_hatch=args.hatch),
    )
    _log_outputs(
        "gt10_vs_full",
        plot_gt10_vs_full_panels(
            grid_config,
            refresh=args.refresh,
            gt10_dir=args.gt10_dir,
            full_dir=args.full_dir,
            draw_hatch=args.hatch,
        ),
    )


if __name__ == "__main__":
    main()
