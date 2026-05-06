"""Plot comparison global maps: Quantile vs PWM exceedance rates.

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
    uv run python scripts/plot_comparison_global.py
    uv run python scripts/plot_comparison_global.py --refresh
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.env import load_env
from lakesource.provider import create_provider
from lakeviz.config import GlobalGridConfig
from lakeviz.comparison import (
    plot_comparison_exceedance_maps,
    plot_comparison_exceedance_panel,
)
from lakeviz.plot_config import setup_chinese_font

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot comparison global maps.")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--sample-file", type=Path, default=DATA_DIR / "comparison" / "sample_lakes.parquet")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures")
    parser.add_argument("--resolution", type=float, default=0.5)
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = parse_args()
    load_env()
    setup_chinese_font()

    source = SourceConfig()
    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=args.resolution, output_dir=args.output_dir)

    sample_ids = None
    if args.sample_file.exists():
        sample_df = pd.read_parquet(args.sample_file)
        sample_ids = set(sample_df["hylak_id"].astype(int))
        log.info("Loaded %d sample lake IDs", len(sample_ids))

    paths = plot_comparison_exceedance_maps(
        grid_config,
        sample_ids=sample_ids,
        refresh=args.refresh,
    )
    if paths:
        log.info("Generated %d comparison maps", len(paths))
    else:
        log.warning("No comparison maps generated")

    panel_paths = plot_comparison_exceedance_panel(
        grid_config,
        sample_ids=sample_ids,
        refresh=args.refresh,
    )
    for pp in panel_paths:
        if pp and pp.exists():
            log.info("Generated comparison panel → %s", pp)


if __name__ == "__main__":
    main()
