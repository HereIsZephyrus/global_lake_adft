"""Sample test lakes with geographic stratification.

Divides the globe into 6°×5° (lon×lat) grid cells, filters out empty
cells, then uniformly samples lakes so each cell contributes roughly the
same number of lakes.  Cells with fewer lakes than the base quota
contribute all their lakes; the shortfall is redistributed proportionally
to cells with surplus.

Output
------
- sample_lakes.parquet : hylak_id, lat, lon, climate_zone, continent,
                         grid_lat, grid_lon
- sample_statistics.parquet : per-grid-cell sampling statistics

Usage
-----
    python scripts/sample_test_lakes.py --n-samples 50000 --output-dir data/comparison
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd

from lakesource.postgres import series_db

log = logging.getLogger(__name__)

GRID_LON = 6
GRID_LAT = 5


def _fetch_valid_lakes() -> pd.DataFrame:
    sql = """
        SELECT li.hylak_id,
               ST_Y(li.centroid) AS lat,
               ST_X(li.centroid) AS lon,
               li.climate_zone,
               li.continent
        FROM lake_info li
        JOIN area_quality aq ON li.hylak_id = aq.hylak_id
        ORDER BY li.hylak_id
    """
    with series_db.connection_context() as conn:
        return pd.read_sql(sql, conn)


def _assign_grid(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["grid_lat"] = (df["lat"] // GRID_LAT * GRID_LAT).astype(int)
    df["grid_lon"] = (df["lon"] // GRID_LON * GRID_LON).astype(int)
    return df


def _uniform_sample(df: pd.DataFrame, n_samples: int, seed: int = 42) -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(seed)

    cell_counts = df.groupby(["grid_lat", "grid_lon"]).size()
    n_cells = len(cell_counts)
    base_quota = n_samples // n_cells

    quota_map: dict[tuple[int, int], int] = {}
    shortfall = 0
    surplus_cells: list[tuple[int, int]] = []

    for cell, cnt in cell_counts.items():
        if cnt <= base_quota:
            quota_map[cell] = int(cnt)
            shortfall += base_quota - cnt
        else:
            quota_map[cell] = base_quota
            surplus_cells.append(cell)

    if surplus_cells and shortfall > 0:
        extra_per = shortfall / len(surplus_cells)
        extra_floor = int(extra_per)
        extra_remainder = shortfall - extra_floor * len(surplus_cells)
        for i, cell in enumerate(surplus_cells):
            quota_map[cell] = base_quota + extra_floor + (1 if i < extra_remainder else 0)

    sampled_parts: list[pd.DataFrame] = []
    stats_parts: list[dict] = []

    for (glat, glon), group in df.groupby(["grid_lat", "grid_lon"]):
        quota = quota_map[(glat, glon)]
        cnt = len(group)
        if quota >= cnt:
            sampled = group
        else:
            sampled = group.sample(n=quota, random_state=rng)
        sampled_parts.append(sampled)
        stats_parts.append({
            "grid_lat": glat,
            "grid_lon": glon,
            "lake_count": cnt,
            "quota": quota,
            "sampled": len(sampled),
        })

    sampled_df = pd.concat(sampled_parts, ignore_index=True)
    stats_df = pd.DataFrame(stats_parts)
    return sampled_df, stats_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample test lakes with geographic stratification.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--n-samples", type=int, default=50_000,
        help="Target total number of sampled lakes.",
    )
    parser.add_argument(
        "--grid-lon", type=int, default=GRID_LON,
        help="Grid cell width in degrees of longitude.",
    )
    parser.add_argument(
        "--grid-lat", type=int, default=GRID_LAT,
        help="Grid cell height in degrees of latitude.",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--output-dir", type=str, default="data/comparison",
        help="Output directory for Parquet files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Fetching valid lakes from database ...")
    df = _fetch_valid_lakes()
    log.info("Total valid lakes: %d", len(df))

    log.info("Assigning %d°×%d° grid cells ...", args.grid_lon, args.grid_lat)
    df = _assign_grid(df)
    n_cells = df.groupby(["grid_lat", "grid_lon"]).ngroups
    log.info("Non-empty grid cells: %d", n_cells)

    log.info("Uniform sampling %d lakes across %d cells ...", args.n_samples, n_cells)
    sampled_df, stats_df = _uniform_sample(df, args.n_samples, seed=args.seed)
    log.info("Sampled %d lakes", len(sampled_df))

    sample_path = output_dir / "sample_lakes.parquet"
    stats_path = output_dir / "sample_statistics.parquet"
    sampled_df.to_parquet(sample_path, index=False)
    stats_df.to_parquet(stats_path, index=False)
    log.info("Wrote %s (%d rows)", sample_path, len(sampled_df))
    log.info("Wrote %s (%d rows)", stats_path, len(stats_df))

    print(f"\n{'='*60}")
    print(f"Sampling summary")
    print(f"{'='*60}")
    print(f"Grid size:       {args.grid_lon}°×{args.grid_lat}° (lon×lat)")
    print(f"Valid cells:     {n_cells}")
    print(f"Base quota:      {args.n_samples // n_cells}")
    print(f"Total sampled:   {len(sampled_df)}")
    print(f"Output:          {sample_path}")
    print(f"Statistics:      {stats_path}")

    by_continent = sampled_df["continent"].value_counts()
    print(f"\nBy continent:")
    for c, n in by_continent.items():
        print(f"  {c}: {n:,}")

    by_climate = sampled_df["climate_zone"].value_counts()
    print(f"\nBy climate zone:")
    for c, n in by_climate.items():
        print(f"  {c}: {n:,}")


if __name__ == "__main__":
    main()
