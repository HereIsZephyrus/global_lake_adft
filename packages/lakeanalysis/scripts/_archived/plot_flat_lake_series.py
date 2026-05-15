"""Visualize water_area series for lakes with few distinct values.

3x4 grid of lake area timelines, sorted by n_distinct ascending.

Usage:
    DATA_BACKEND=parquet PARQUET_DATA_DIR=data/parquet \
    uv run python scripts/plot_flat_lake_series.py
    uv run python scripts/plot_flat_lake_series.py --n-distinct 10
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from lakeanalysis.logger import Logger

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot flat lake area series.")
    parser.add_argument("--n-distinct", type=int, default=5, help="Max distinct water_area values to include.")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory for figures.")
    return parser.parse_args()


def main() -> None:
    Logger("plot_flat_lake_series")
    args = parse_args()

    import matplotlib.pyplot as plt
    from lakeviz.style.presets import Theme
    from lakesource.config import SourceConfig
    Theme.apply()

    config = SourceConfig()
    parquet_dir = config.data_dir

    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = parquet_dir.parent / "figures" / "quality"
    output_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute("SET threads=1")
    con.execute(f"CREATE VIEW lake_area AS SELECT * FROM read_parquet('{parquet_dir}/lake_area/*.parquet')")
    con.execute(f"CREATE VIEW area_quality AS SELECT * FROM read_parquet('{parquet_dir}/area_quality/*.parquet')")

    lakes = con.execute(f"""
        WITH lake_stats AS (
            SELECT
                hylak_id,
                COUNT(DISTINCT water_area) AS n_distinct,
                AVG(water_area) AS mean_area
            FROM lake_area
            WHERE hylak_id IN (SELECT hylak_id FROM area_quality)
            GROUP BY hylak_id
            HAVING COUNT(DISTINCT water_area) <= {args.n_distinct}
        )
        SELECT hylak_id, n_distinct, mean_area
        FROM lake_stats
        ORDER BY n_distinct ASC, mean_area DESC
        LIMIT 12
    """).df()

    if lakes.empty:
        log.warning("No lakes found with n_distinct <= %d", args.n_distinct)
        return

    log.info("Selected %d lakes", len(lakes))

    fig, axes = plt.subplots(3, 4, figsize=(20, 10), sharex=True)

    for idx, (_, row) in enumerate(lakes.iterrows()):
        ax = axes.flat[idx]
        hid = int(row["hylak_id"])
        n_dist = int(row["n_distinct"])

        series = con.execute(f"""
            SELECT year_month, water_area
            FROM lake_area
            WHERE hylak_id = {hid}
            ORDER BY year_month
        """).df()

        dates = pd.to_datetime(series["year_month"])
        ax.plot(dates, series["water_area"], linewidth=0.8, color="#5ab4ac")
        ax.set_title(f"hylak_id={hid}\nn_distinct={n_dist}, mean={row['mean_area']:.0f}", fontsize=9)
        ax.tick_params(labelsize=7)
        ax.grid(alpha=0.2)

    for idx in range(len(lakes), 12):
        axes.flat[idx].set_visible(False)

    fig.suptitle(f"Flat Lake Area Series (n_distinct ≤ {args.n_distinct})", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"flat_lake_series_ndist{args.n_distinct}.png"
    fig.savefig(path, dpi=200, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved: %s", path)

    con.close()


if __name__ == "__main__":
    main()