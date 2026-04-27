"""Plot sample lakes with true-linear interpolation segments.

Reads interpolation_detect table from PostgreSQL, samples lakes
with n_linear_segments > 0 that have NO frozen months (per anomaly
table), and generates figures (2x3 grid each) showing time series
with collinear segments highlighted.

Usage:
    uv run python scripts/plot_interpolation_sample.py
    uv run python scripts/plot_interpolation_sample.py --n-samples 120
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakesource.config import Backend, SourceConfig
from lakesource.postgres import series_db
from lakesource.provider.factory import create_provider
from lakeanalysis.logger import Logger
from lakeanalysis.quality.interpolation import (
    InterpolationConfig,
    get_collinear_segments,
)
from lakeviz.domain.interpolation import draw_interpolation_timeline

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_DATA_DIR = Path("/mnt/repo/lake/global_lake_adft/data")

NROWS = 2
NCOLS = 3
N_PER_FIG = NROWS * NCOLS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plot sample lakes with interpolation segments."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Path to data directory.",
    )
    parser.add_argument(
        "--n-samples",
        type=int,
        default=120,
        help="Number of lakes to sample (default: 120).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for sampling.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory for figures.",
    )
    return parser.parse_args()


def fetch_no_frozen_linear_lake_ids(conn, n_samples: int, seed: int) -> list[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT i.hylak_id
            FROM interpolation_detect i
            WHERE i.n_linear_segments > 0
              AND NOT EXISTS (
                  SELECT 1 FROM anomaly a
                  WHERE a.hylak_id = i.hylak_id
                    AND a.anomaly_type = 'frozen'
              )
            ORDER BY i.hylak_id
            """
        )
        all_ids = [row[0] for row in cur.fetchall()]

    log.info("Found %d no-frozen lakes with true-linear segments", len(all_ids))

    if len(all_ids) <= n_samples:
        return all_ids
    rng = np.random.default_rng(seed)
    return [int(x) for x in rng.choice(all_ids, size=n_samples, replace=False)]


def run(
    data_dir: Path | None = None,
    n_samples: int = 120,
    seed: int = 42,
    output_dir: Path | None = None,
) -> None:
    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    parquet_dir = data_dir / "parquet"

    if output_dir is None:
        output_dir = data_dir / "interpolation" / "figures"
    output_dir.mkdir(parents=True, exist_ok=True)

    source_config = SourceConfig(backend=Backend.PARQUET, data_dir=parquet_dir)
    provider = create_provider(source_config)

    with series_db.connection_context() as conn:
        lake_ids = fetch_no_frozen_linear_lake_ids(conn, n_samples, seed)

    log.info("Sampled %d lakes", len(lake_ids))

    config = InterpolationConfig()
    n_figs = (len(lake_ids) + N_PER_FIG - 1) // N_PER_FIG

    for fig_idx in range(n_figs):
        start = fig_idx * N_PER_FIG
        end = min(start + N_PER_FIG, len(lake_ids))
        batch_ids = lake_ids[start:end]

        fig, axes = plt.subplots(NROWS, NCOLS, figsize=(15, 8))
        axes = axes.flatten()

        for i, hylak_id in enumerate(batch_ids):
            ax = axes[i]
            hid = int(hylak_id)

            lake_frames = provider.fetch_lake_area_by_ids([hid])
            frozen_map = provider.fetch_frozen_year_months_by_ids([hid])

            if hid not in lake_frames:
                ax.text(0.5, 0.5, f"No data for {hid}", ha="center", va="center")
                continue

            df = lake_frames[hid]
            frozen = frozen_map.get(hid)

            segments = get_collinear_segments(df, frozen_year_months=frozen, config=config)
            seg_dicts = [
                {
                    "start_idx": s.start_idx,
                    "end_idx": s.end_idx,
                    "is_flat": s.is_flat,
                }
                for s in segments
            ]

            draw_interpolation_timeline(ax, df, seg_dicts, hylak_id=hid)

        for i in range(len(batch_ids), len(axes)):
            axes[i].set_visible(False)

        fig.tight_layout()
        out_path = output_dir / f"sample_{fig_idx + 1:02d}.png"
        fig.savefig(out_path, dpi=150)
        plt.close(fig)
        log.info("Saved %s", out_path)

    log.info("Done. Generated %d figures in %s", n_figs, output_dir)


def main() -> None:
    args = parse_args()
    Logger("plot_interpolation_sample")
    run(
        data_dir=args.data_dir,
        n_samples=args.n_samples,
        seed=args.seed,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
