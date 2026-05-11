"""Migrate area-ratio anomalies and backfill anomaly_flags in area_anomalies.

This script:
  1. Scans area_quality for lakes with extreme area ratios
  2. Moves them to area_anomalies with the appropriate anomaly_flags bitmask
  3. Backfills anomaly_flags for existing area_anomalies rows

Usage examples:
    # Inspect what would be migrated:
    uv run python scripts/migrate_area_ratio_to_anomalies.py --dry-run

    # Execute migration:
    uv run python scripts/migrate_area_ratio_to_anomalies.py

    # Test run with id cap and custom thresholds:
    uv run python scripts/migrate_area_ratio_to_anomalies.py \
        --limit-id 50000 \
        --area-ratio-min 0.1 \
        --area-ratio-max 10.0
"""

from __future__ import annotations

import argparse
import logging

from lakesource.postgres import (
    ensure_area_anomalies_table,
    ensure_area_quality_table,
    fetch_atlas_area_chunk,
    fetch_lake_area_chunk,
    move_area_quality_to_anomalies,
    series_db,
    upsert_area_anomalies,
)
from lakeanalysis.logger import Logger
from lakeanalysis.quality import (
    AreaRatioConfig,
    FlatnessFilterConfig,
    LakeContext,
    classify_area_anomaly,
    compute_mean_area,
    compute_median_area,
    decode_anomaly_flags,
    default_filters,
    FLAG_MEDIAN_ZERO,
)

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate area-ratio anomalies and backfill anomaly_flags."
    )
    parser.add_argument(
        "--limit-id",
        type=int,
        default=None,
        metavar="N",
        help="Only scan hylak_id < N.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        metavar="N",
        help="Number of hylak_id values scanned per chunk.",
    )
    parser.add_argument(
        "--area-ratio-min",
        type=float,
        default=0.1,
        metavar="R",
        help="Minimum acceptable ratio (default: 0.1).",
    )
    parser.add_argument(
        "--area-ratio-max",
        type=float,
        default=10.0,
        metavar="R",
        help="Maximum acceptable ratio (default: 10.0).",
    )
    parser.add_argument(
        "--flat-dominant-ratio-threshold",
        type=float,
        default=0.8,
        metavar="R",
        help="Flatness filter: dominant value frequency ratio threshold.",
    )
    parser.add_argument(
        "--move-batch-size",
        type=int,
        default=5000,
        metavar="N",
        help="Batch size for DB migration operations.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only detect candidates, do not migrate.",
    )
    parser.add_argument(
        "--skip-backfill",
        action="store_true",
        help="Skip backfilling anomaly_flags for existing area_anomalies rows.",
    )
    return parser.parse_args()


def _iter_batches(items: list[int], batch_size: int) -> list[list[int]]:
    return [items[i : i + batch_size] for i in range(0, len(items), batch_size)]


def run(
    limit_id: int | None,
    chunk_size: int,
    ratio_config: AreaRatioConfig,
    flat_config: FlatnessFilterConfig,
    move_batch_size: int,
    dry_run: bool,
    skip_backfill: bool,
) -> None:
    """Migrate area-ratio anomalies and backfill anomaly_flags."""
    with series_db.connection_context() as conn:
        ensure_area_quality_table(conn)
        ensure_area_anomalies_table(conn)

    filters = default_filters(flat_config=flat_config, ratio_config=ratio_config)

    # Phase 1: Scan area_quality for ratio anomalies
    ratio_candidates: list[int] = []
    all_quality_data: dict[int, dict] = {}

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area "
                "FROM area_quality "
                + ("WHERE hylak_id < %(lim)s " if limit_id else "")
                + "ORDER BY hylak_id",
                {"lim": limit_id} if limit_id else {},
            )
            rows = cur.fetchall()

    for row in rows:
        hid = int(row[0])
        rs_mean = float(row[1]) if row[1] else 0.0
        rs_median = float(row[2]) if row[2] else 0.0
        atlas = float(row[3]) if row[3] else 0.0
        all_quality_data[hid] = {
            "rs_area_mean": rs_mean,
            "rs_area_median": rs_median,
            "atlas_area": atlas,
        }

        ctx = LakeContext(
            df=None,
            df_no_frozen=None,
            rs_area_median=rs_median,
            rs_area_mean=rs_mean,
            rs_area_quantile=rs_median,
            atlas_area=atlas,
        )
        ratio_filter = filters[2]  # AreaRatioFilter
        flag = ratio_filter.classify(ctx)
        if flag.is_anomaly:
            ratio_candidates.append(hid)

    log.info(
        "Scanned %d area_quality rows, found %d ratio anomalies",
        len(all_quality_data),
        len(ratio_candidates),
    )

    # Phase 2: Move ratio anomalies to area_anomalies
    if ratio_candidates and not dry_run:
        for batch in _iter_batches(ratio_candidates, move_batch_size):
            with series_db.connection_context() as conn:
                moved = move_area_quality_to_anomalies(conn, batch)
            log.info("Moved batch: %d rows", moved)

        # Now backfill anomaly_flags for moved rows
        for hid in ratio_candidates:
            data = all_quality_data[hid]
            flags = 0
            if data["rs_area_median"] == 0.0:
                flags |= FLAG_MEDIAN_ZERO
            flags |= 4  # FLAG_AREA_RATIO

            with series_db.connection_context() as conn:
                upsert_area_anomalies(conn, [{
                    "hylak_id": hid,
                    "rs_area_mean": data["rs_area_mean"],
                    "rs_area_median": data["rs_area_median"],
                    "atlas_area": data["atlas_area"],
                    "anomaly_flags": flags,
                }])

        log.info("Moved and flagged %d ratio anomalies", len(ratio_candidates))
    elif ratio_candidates and dry_run:
        log.info("Dry run: would move %d ratio anomalies", len(ratio_candidates))

    # Phase 3: Backfill anomaly_flags for existing area_anomalies rows
    if skip_backfill:
        log.info("Skipping backfill of existing area_anomalies rows")
        return

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT hylak_id, rs_area_mean, rs_area_median, atlas_area "
                "FROM area_anomalies "
                + ("WHERE hylak_id < %(lim)s " if limit_id else "")
                + "ORDER BY hylak_id",
                {"lim": limit_id} if limit_id else {},
            )
            anomaly_rows = cur.fetchall()

    backfilled = 0
    for row in anomaly_rows:
        hid = int(row[0])
        rs_mean = float(row[1]) if row[1] else 0.0
        rs_median = float(row[2]) if row[2] else 0.0
        atlas = float(row[3]) if row[3] else 0.0

        flags = 0
        if rs_median == 0.0:
            flags |= FLAG_MEDIAN_ZERO

        # For flat and outside_range, we need the original time series
        # Only median_zero and area_ratio can be determined without it
        # area_ratio: check if already flagged (from Phase 2)
        if hid in set(ratio_candidates):
            flags |= 4  # FLAG_AREA_RATIO

        if flags == 0:
            continue

        if not dry_run:
            with series_db.connection_context() as conn:
                upsert_area_anomalies(conn, [{
                    "hylak_id": hid,
                    "rs_area_mean": rs_mean,
                    "rs_area_median": rs_median,
                    "atlas_area": atlas,
                    "anomaly_flags": flags,
                }])
        backfilled += 1

    log.info(
        "Backfilled anomaly_flags for %d existing area_anomalies rows%s",
        backfilled,
        " (dry run)" if dry_run else "",
    )


def main() -> None:
    args = parse_args()
    Logger("migrate_area_ratio_to_anomalies")
    ratio_config = AreaRatioConfig(
        min_ratio=args.area_ratio_min,
        max_ratio=args.area_ratio_max,
    )
    flat_config = FlatnessFilterConfig(
        dominant_ratio_threshold=args.flat_dominant_ratio_threshold,
    )
    run(
        limit_id=args.limit_id,
        chunk_size=args.chunk_size,
        ratio_config=ratio_config,
        flat_config=flat_config,
        move_batch_size=args.move_batch_size,
        dry_run=args.dry_run,
        skip_backfill=args.skip_backfill,
    )


if __name__ == "__main__":
    main()
