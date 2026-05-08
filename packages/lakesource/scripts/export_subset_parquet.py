"""Export a lake-area subset from existing Parquet files to a new directory.

Usage:
    python export_subset_parquet.py [--input-dir DIR] [--output-dir DIR] [--min-area SQKM]

Default source: data/parquet/
Default output: data/parquet_gt10/
Default filter: atlas_area > 10 (km²)

Reads hylak_id set from area_quality parquet (atlas_area > threshold),
then filters every table by those IDs.  No PostgreSQL connection needed.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import duckdb

log = logging.getLogger(__name__)

DEFAULT_TABLES = [
    "lake_area",
    "lake_info",
    "anomaly",
    "area_quality",
    "area_anomalies",
    "af_nearest",
]


def _table_glob(input_dir: Path, table: str) -> str:
    """Return the read_parquet glob for a table."""
    dir_path = input_dir / table
    if dir_path.is_dir():
        return f"{dir_path}/*.parquet"
    single = input_dir / f"{table}.parquet"
    if single.exists():
        return str(single)
    raise FileNotFoundError(f"Neither {dir_path}/ nor {single} found")


def get_subset_ids(input_dir: Path, min_area: float) -> set[int]:
    con = duckdb.connect(":memory:")
    ids: set[int] = set()
    for source in ("area_quality", "area_anomalies"):
        glob = _table_glob(input_dir, source)
        df = con.execute(
            f"SELECT DISTINCT hylak_id FROM read_parquet('{glob}') WHERE atlas_area > {min_area}"
        ).fetchdf()
        ids.update(df["hylak_id"].astype(int))
    con.close()
    return ids


def export_table(
    table: str, input_dir: Path, output_dir: Path, id_set: set[int]
) -> int:
    """Read a table from input parquet, filter by id_set, write to output."""
    out_path = output_dir / f"{table}.parquet"
    glob = _table_glob(input_dir, table)
    id_list = list(id_set)

    start = time.time()
    con = duckdb.connect(":memory:")

    # Build IN clause; DuckDB can handle large lists efficiently
    ids_str = ", ".join(str(i) for i in id_list)
    sql = f"SELECT * FROM read_parquet('{glob}') WHERE hylak_id IN ({ids_str})"
    df = con.execute(sql).fetchdf()
    con.close()

    df.to_parquet(out_path, index=False)
    elapsed = time.time() - start
    log.info("  %s: %d rows in %.2fs → %s", table, len(df), elapsed, out_path)
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export a lake-area subset from existing Parquet files"
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("data/parquet"),
        help="Source parquet directory",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/parquet_gt10"),
        help="Output parquet directory",
    )
    parser.add_argument(
        "--min-area",
        type=float,
        default=10.0,
        help="Minimum atlas_area (km²) threshold",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=None,
        help="Tables to export (default: lake_area, lake_info, anomaly, area_quality, area_anomalies, af_nearest)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    log.info("Loading hylak_id subset (atlas_area > %.1f km²) ...", args.min_area)
    id_set = get_subset_ids(args.input_dir, args.min_area)
    log.info("Subset: %d lakes with atlas_area > %.1f km²", len(id_set), args.min_area)

    tables = args.tables or DEFAULT_TABLES
    total = 0
    for table in tables:
        log.info("Exporting %s ...", table)
        try:
            n = export_table(table, args.input_dir, args.output_dir, id_set)
            total += n
        except Exception as exc:
            log.error("Failed to export %s: %s", table, exc)

    log.info("Export complete: %s (%d rows total)", args.output_dir, total)


if __name__ == "__main__":
    main()
