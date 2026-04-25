"""Export PostgreSQL tables to Parquet files for offline use.

Usage:
    python export_to_parquet.py [--output-dir DIR] [--tables TABLE ...]

Default output: data/parquet/
Default tables: all tables listed in config.toml [tables.parquet]

The export also pre-computes lat/lon columns from PostGIS centroids
in lake_info, so that ParquetLakeProvider can run grid aggregation
queries without PostGIS.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.provider import create_provider

log = logging.getLogger(__name__)

DEFAULT_TABLES = [
    "lake_area",
    "lake_info",
    "anomaly",
    "area_quality",
    "af_nearest",
    "eot_extremes",
    "eot_results",
    "eot_run_status",
    "hawkes_results",
    "quantile_extremes",
    "quantile_abrupt_transitions",
    "quantile_labels",
    "quantile_run_status",
    "pwm_extreme_thresholds",
    "pwm_extreme_run_status",
]


def export_table(provider, table: str, output_dir: Path) -> None:
    out_path = output_dir / f"{table}.parquet"
    if out_path.exists():
        log.info("SKIP %s (already exists)", out_path)
        return

    log.info("Exporting %s ...", table)
    start = time.time()

    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    df.to_parquet(out_path, index=False)
    elapsed = time.time() - start
    log.info("  %s: %d rows in %.2fs → %s", table, len(df), elapsed, out_path)


def export_lake_info_with_coords(output_dir: Path) -> None:
    out_path = output_dir / "lake_info.parquet"
    if out_path.exists():
        log.info("SKIP lake_info (already exists)")
        return

    log.info("Exporting lake_info with pre-computed lat/lon ...")
    start = time.time()

    from lakesource.postgres import series_db

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT *,
                       ST_Y(centroid) AS lat,
                       ST_X(centroid) AS lon
                FROM lake_info
            """)
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]

    df = pd.DataFrame(rows, columns=colnames)
    drop_cols = [c for c in ("centroid", "geometry") if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)
    df.to_parquet(out_path, index=False)
    elapsed = time.time() - start
    log.info("  lake_info: %d rows, %d cols in %.2fs → %s", len(df), len(df.columns), elapsed, out_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export PostgreSQL tables to Parquet")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/parquet"),
        help="Output directory for parquet files",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Tables to export (default: all)",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Overwrite existing parquet files",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    tables = args.tables or DEFAULT_TABLES

    if args.refresh:
        for p in args.output_dir.glob("*.parquet"):
            p.unlink()
            log.info("Deleted %s", p)

    export_lake_info_with_coords(args.output_dir)

    provider = create_provider(SourceConfig())
    for table in tables:
        if table == "lake_info":
            continue
        try:
            export_table(provider, table, args.output_dir)
        except Exception as exc:
            log.error("Failed to export %s: %s", table, exc)

    log.info("Export complete: %s", args.output_dir)


if __name__ == "__main__":
    main()
