"""Plot comparison zonal (latitude-profile) figures.

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
    uv run python scripts/plot_comparison_zonal.py
    uv run python scripts/plot_comparison_zonal.py --lat-band 10
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
from lakeviz.comparison import plot_comparison_zonal_profile
from lakeviz.style.presets import Theme
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)

DATA_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot comparison zonal latitude profiles.")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--sample-file", type=Path, default=DATA_DIR / "comparison" / "sample_lakes.parquet")
    parser.add_argument("--output-dir", type=Path, default=DATA_DIR / "figures")
    parser.add_argument("--resolution", type=float, default=0.5)
    parser.add_argument("--lat-band", type=float, default=5.0, help="Latitude band size in degrees.")
    parser.add_argument("--min-lakes", type=int, default=1, help="Minimum lake count per grid cell.")
    return parser.parse_args()


def main() -> None:
    Logger("plot_comparison_zonal")
    args = parse_args()
    load_env()
    Theme.apply()

    source = SourceConfig()
    provider = create_provider(source)
    grid_config = GlobalGridConfig(provider=provider, resolution=args.resolution, output_dir=args.output_dir)

    sample_ids = None
    if args.sample_file.exists():
        sample_df = pd.read_parquet(args.sample_file)
        sample_ids = set(sample_df["hylak_id"].astype(int))
        log.info("Loaded %d sample lake IDs", len(sample_ids))

    paths = plot_comparison_zonal_profile(
        grid_config,
        sample_ids=sample_ids,
        refresh=args.refresh,
        min_lakes=args.min_lakes,
        lat_band_size=args.lat_band,
    )
    if paths:
        log.info("Generated %d zonal profile figures", len(paths))
    else:
        log.warning("No zonal profile figures generated")


if __name__ == "__main__":
    main()
