"""Export PostgreSQL input tables to Parquet for a lake-area subset.

Usage:
    python export_subset_parquet.py [--output-dir DIR] [--min-area SQKM] [--refresh]

Default output: data/parquet_gt10/
Default filter: lake_info.lake_area > 10 (km²)

Only lakes whose atlas lake_area exceeds the threshold are exported.
All tables are filtered by the resulting hylak_id set.

Large tables are exported in chunks (by hylak_id range) to subdirectories.
Small tables are exported as single files.

lake_info gets special handling: ST_Y/X(centroid) → lat/lon columns,
and PostGIS geometry columns are dropped before writing.
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

import pandas as pd


log = logging.getLogger(__name__)

CHUNK_SIZE = 200_000
SMALL_CHUNK_SIZE = 10_000

MONTHLY_TABLES = {
    "lake_area",
    "anomaly",
}

LARGE_TABLES = {
    "area_anomalies",
    "area_quality",
    "lake_info",
    "lake_area",
    "anomaly",
}

DEFAULT_TABLES = [
    "lake_area",
    "lake_info",
    "anomaly",
    "area_quality",
    "area_anomalies",
    "af_nearest",
]


def _get_conn():
    from lakesource.postgres import series_db
    return series_db.connection_context()


def _get_subset_ids(min_area: float) -> set[int]:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hylak_id FROM lake_info WHERE lake_area > %s",
                (min_area,),
            )
            return {row[0] for row in cur.fetchall()}


def _get_max_hylak_id(table: str) -> int:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COALESCE(MAX(hylak_id), 0) FROM {table}")
            return int(cur.fetchone()[0])


def _read_chunk_filtered(
    table: str, chunk_start: int, chunk_end: int, id_set: set[int]
) -> pd.DataFrame:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT * FROM {table} WHERE hylak_id >= %s AND hylak_id < %s",
                (chunk_start, chunk_end),
            )
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=colnames)
    if not df.empty:
        df = df[df["hylak_id"].isin(id_set)]
    return df


def _read_full_filtered(table: str, id_set: set[int]) -> pd.DataFrame:
    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            colnames = [d.name for d in cur.description]
    df = pd.DataFrame(rows, columns=colnames)
    if not df.empty:
        df = df[df["hylak_id"].isin(id_set)]
    return df


def export_lake_info(
    output_dir: Path, id_set: set[int], *, refresh: bool = False
) -> None:
    table_dir = output_dir / "lake_info"
    table_dir.mkdir(parents=True, exist_ok=True)

    max_id = _get_max_hylak_id("lake_info")
    total_rows = 0
    start = time.time()

    for cs in range(0, max_id + 1, CHUNK_SIZE):
        ce = cs + CHUNK_SIZE
        chunk_path = table_dir / f"{cs:06d}.parquet"
        if not refresh and chunk_path.exists():
            log.info("  SKIP lake_info chunk [%d, %d)", cs, ce)
            continue

        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *,
                           ST_Y(centroid) AS lat,
                           ST_X(centroid) AS lon
                    FROM lake_info
                    WHERE hylak_id >= %s AND hylak_id < %s
                    """,
                    (cs, ce),
                )
                rows = cur.fetchall()
                colnames = [d.name for d in cur.description]

        df = pd.DataFrame(rows, columns=colnames)
        drop_cols = [c for c in ("centroid", "geometry") if c in df.columns]
        if drop_cols:
            df = df.drop(columns=drop_cols)
        if not df.empty:
            df = df[df["hylak_id"].isin(id_set)]
        if df.empty:
            continue
        df.to_parquet(chunk_path, index=False)
        total_rows += len(df)
        log.info("  lake_info [%d, %d): %d rows", cs, ce, len(df))

    elapsed = time.time() - start
    log.info("  lake_info: %d rows total in %.2fs", total_rows, elapsed)


def export_large_table(
    table: str, output_dir: Path, id_set: set[int], *, refresh: bool = False
) -> None:
    table_dir = output_dir / table
    table_dir.mkdir(parents=True, exist_ok=True)

    chunk_size = SMALL_CHUNK_SIZE if table in MONTHLY_TABLES else CHUNK_SIZE
    max_id = _get_max_hylak_id(table)
    total_rows = 0
    start = time.time()

    for cs in range(0, max_id + 1, chunk_size):
        ce = cs + chunk_size
        chunk_path = table_dir / f"{cs:06d}.parquet"
        if not refresh and chunk_path.exists():
            log.info("  SKIP %s chunk [%d, %d)", table, cs, ce)
            continue

        df = _read_chunk_filtered(table, cs, ce, id_set)
        if df.empty:
            continue
        df.to_parquet(chunk_path, index=False)
        total_rows += len(df)
        log.info("  %s [%d, %d): %d rows", table, cs, ce, len(df))

    elapsed = time.time() - start
    log.info("  %s: %d rows total in %.2fs", table, total_rows, elapsed)


def export_small_table(
    table: str, output_dir: Path, id_set: set[int], *, refresh: bool = False
) -> None:
    out_path = output_dir / f"{table}.parquet"
    if not refresh and out_path.exists():
        log.info("SKIP %s (already exists)", table)
        return

    start = time.time()
    df = _read_full_filtered(table, id_set)
    df.to_parquet(out_path, index=False)
    elapsed = time.time() - start
    log.info("  %s: %d rows in %.2fs → %s", table, len(df), elapsed, out_path)


def export_table(
    table: str, output_dir: Path, id_set: set[int], *, refresh: bool = False
) -> None:
    if table == "lake_info":
        export_lake_info(output_dir, id_set, refresh=refresh)
    elif table in LARGE_TABLES:
        export_large_table(table, output_dir, id_set, refresh=refresh)
    else:
        export_small_table(table, output_dir, id_set, refresh=refresh)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export PostgreSQL tables to Parquet for a lake-area subset"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/parquet_gt10"),
        help="Output directory for parquet files",
    )
    parser.add_argument(
        "--min-area",
        type=float,
        default=10.0,
        help="Minimum atlas lake_area (km²) threshold",
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
        help="Overwrite existing files (default: resume/skip)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    args.output_dir.mkdir(parents=True, exist_ok=True)

    if args.refresh:
        for p in sorted(args.output_dir.rglob("*.parquet")):
            p.unlink()
            log.info("Deleted %s", p)
        for d in sorted(args.output_dir.iterdir()):
            if d.is_dir():
                d.rmdir() if not any(d.iterdir()) else None

    log.info("Loading hylak_id subset (lake_area > %.1f km²) ...", args.min_area)
    id_set = _get_subset_ids(args.min_area)
    log.info("Subset: %d lakes with lake_area > %.1f km²", len(id_set), args.min_area)

    tables = args.tables or DEFAULT_TABLES

    for table in tables:
        log.info("Exporting %s ...", table)
        try:
            export_table(table, args.output_dir, id_set, refresh=args.refresh)
        except Exception as exc:
            log.error("Failed to export %s: %s", table, exc)

    log.info("Export complete: %s", args.output_dir)


if __name__ == "__main__":
    main()
