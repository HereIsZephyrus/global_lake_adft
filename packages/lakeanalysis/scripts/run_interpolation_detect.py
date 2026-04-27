"""Detect linear interpolation in lake area time series.

For each lake, checks whether the water_area monthly series contains
collinear segments (3+ consecutive points with identical diffs),
after excluding frozen months and zero-area observations.

Segments are classified as:
  - "flat": all diffs ≈ 0 (constant value)
  - "linear": non-zero constant diffs (true linear interpolation)

Only lakes with n_linear_segments > 0 are written to PostgreSQL.
All results are written to parquet as backup.

Usage:
    uv run python scripts/run_interpolation_detect.py
    uv run python scripts/run_interpolation_detect.py --limit-id 5000
    uv run python scripts/run_interpolation_detect.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from lakesource.config import Backend, SourceConfig
from lakesource.postgres import (
    ensure_interpolation_detect_table,
    series_db,
    upsert_interpolation_detect,
)
from lakesource.provider.factory import create_provider
from lakeanalysis.logger import Logger
from lakeanalysis.quality.interpolation import InterpolationConfig, detect_interpolation

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = DATA_DIR / "interpolation"
DEFAULT_DATA_DIR = Path("/mnt/repo/lake/global_lake_adft/data")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect linear interpolation in lake area time series."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Path to data directory (default: /mnt/repo/lake/global_lake_adft/data).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values per chunk (default: 10000).",
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only process lakes with hylak_id < N.",
    )
    parser.add_argument(
        "--id-start",
        type=int,
        default=0,
        metavar="N",
        help="Start of hylak_id range (default: 0).",
    )
    parser.add_argument(
        "--id-end",
        type=int,
        default=None,
        metavar="N",
        help="End of hylak_id range (exclusive).",
    )
    parser.add_argument(
        "--min-collinear-points",
        type=int,
        default=3,
        metavar="N",
        help="Minimum consecutive collinear points to flag as interpolation (default: 3).",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip writing to PostgreSQL (only write parquet).",
    )
    return parser.parse_args()


def run(
    data_dir: Path | None = None,
    chunk_size: int = 10_000,
    limit_id: int | None = None,
    id_start: int = 0,
    id_end: int | None = None,
    min_collinear_points: int = 3,
    no_db: bool = False,
) -> pd.DataFrame:
    config = InterpolationConfig(min_collinear_points=min_collinear_points)

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR
    parquet_dir = data_dir / "parquet"
    if not parquet_dir.exists():
        raise FileNotFoundError(f"Parquet data directory not found: {parquet_dir}")

    source_config = SourceConfig(backend=Backend.PARQUET, data_dir=parquet_dir)
    provider = create_provider(source_config)

    max_id = provider.fetch_max_hylak_id()
    if limit_id is not None:
        max_id = min(max_id, limit_id)
    if id_end is not None:
        max_id = min(max_id, id_end)

    log.info(
        "Starting interpolation detection: hylak_id range [%d, %d), chunk_size=%d, min_collinear_points=%d",
        id_start,
        max_id,
        chunk_size,
        config.min_collinear_points,
    )

    if not no_db:
        with series_db.connection_context() as conn:
            ensure_interpolation_detect_table(conn)

    all_rows: list[dict] = []
    db_rows: list[dict] = []
    n_total = 0
    n_linear = 0
    n_flat_only = 0

    chunk_start = id_start
    while chunk_start < max_id:
        chunk_end = min(chunk_start + chunk_size, max_id)
        log.info("Processing chunk [%d, %d)...", chunk_start, chunk_end)

        lake_frames = provider.fetch_lake_area_chunk(chunk_start, chunk_end)
        frozen_map = provider.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

        chunk_linear = 0
        chunk_flat = 0
        for hylak_id, df in lake_frames.items():
            frozen = frozen_map.get(hylak_id)
            result = detect_interpolation(df, frozen_year_months=frozen, config=config)

            all_rows.append(
                {
                    "hylak_id": hylak_id,
                    "has_interpolation": result.has_interpolation,
                    "n_linear_segments": result.n_linear_segments,
                    "n_flat_segments": result.n_flat_segments,
                    "max_linear_len": result.max_linear_len,
                    "max_flat_len": result.max_flat_len,
                    "collinear_ratio": result.collinear_ratio,
                    "first_linear_ym": result.first_linear_ym,
                    "n_obs": result.n_obs,
                }
            )

            if result.n_linear_segments > 0:
                chunk_linear += 1
                db_rows.append(
                    {
                        "hylak_id": hylak_id,
                        "n_linear_segments": result.n_linear_segments,
                        "n_flat_segments": result.n_flat_segments,
                        "max_linear_len": result.max_linear_len,
                        "max_flat_len": result.max_flat_len,
                        "collinear_ratio": result.collinear_ratio,
                        "first_linear_ym": result.first_linear_ym,
                        "n_obs": result.n_obs,
                    }
                )
            elif result.n_flat_segments > 0:
                chunk_flat += 1

        n_total += len(lake_frames)
        n_linear += chunk_linear
        n_flat_only += chunk_flat
        log.info(
            "Chunk [%d, %d): %d lakes, %d true-linear, %d flat-only",
            chunk_start,
            chunk_end,
            len(lake_frames),
            chunk_linear,
            chunk_flat,
        )
        chunk_start = chunk_end

    result_df = pd.DataFrame(all_rows)
    if not result_df.empty:
        result_df = result_df.sort_values("hylak_id").reset_index(drop=True)

    output_dir = data_dir / "interpolation"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / "interpolation_detect.parquet"
    result_df.to_parquet(out_path, index=False)
    log.info("Wrote %d rows to %s", len(result_df), out_path)

    if not no_db and db_rows:
        with series_db.connection_context() as conn:
            upsert_interpolation_detect(conn, db_rows)
        log.info("Wrote %d true-linear lakes to PostgreSQL", len(db_rows))

    if n_total > 0:
        log.info(
            "Summary: %d total, %d true-linear (%.2f%%), %d flat-only (%.2f%%)",
            n_total,
            n_linear,
            100.0 * n_linear / n_total,
            n_flat_only,
            100.0 * n_flat_only / n_total,
        )

    return result_df


def main() -> None:
    args = parse_args()
    Logger("run_interpolation_detect")
    run(
        data_dir=args.data_dir,
        chunk_size=args.chunk_size,
        limit_id=args.limit_id,
        id_start=args.id_start,
        id_end=args.id_end,
        min_collinear_points=args.min_collinear_points,
        no_db=args.no_db,
    )


if __name__ == "__main__":
    main()
