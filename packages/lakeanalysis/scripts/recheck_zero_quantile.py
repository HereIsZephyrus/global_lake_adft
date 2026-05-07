"""Recheck lakes flagged by median_zero using a higher quantile threshold.

For lakes currently in area_anomalies with the zero_quantile (bit 1) flag,
recompute the quantile area at the new threshold.  If the new quantile is
non-zero, re-run all filters and move lakes that are no longer anomalous
to area_quality, or update their anomaly_flags if other filters still
trigger.

Usage:
    # Dry run (stats only):
    uv run python scripts/recheck_zero_quantile.py --dry-run

    # Execute with default p80:
    uv run python scripts/recheck_zero_quantile.py

    # Use p90 instead:
    uv run python scripts/recheck_zero_quantile.py --zero-quantile 0.90

    # Custom chunk size:
    uv run python scripts/recheck_zero_quantile.py --chunk-size 5000
"""

from __future__ import annotations

import argparse
import logging

from lakesource.postgres import (
    fetch_atlas_area_chunk,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_area_quality,
)
from lakeanalysis.logger import Logger
from lakeanalysis.quality import (
    FLAG_ZERO_QUANTILE,
    LakeContext,
    ZeroQuantileConfig,
    classify_area_anomaly,
    compute_mean_area,
    compute_median_area,
    compute_quantile_area,
    default_filters,
    filter_frozen_rows,
)

log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recheck zero-quantile flagged lakes with a higher quantile threshold.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--zero-quantile",
        type=float,
        default=0.80,
        metavar="Q",
        help="Quantile position (0-1) for the zero check (default: 0.80).",
    )
    parser.add_argument("--chunk-size", type=int, default=5_000, metavar="N")
    parser.add_argument("--dry-run", action="store_true", help="Stats only, no writes.")
    return parser.parse_args()


def _load_zero_quantile_lakes(conn) -> dict[int, int]:
    result: dict[int, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hylak_id, anomaly_flags FROM area_anomalies WHERE anomaly_flags & %s > 0",
            [FLAG_ZERO_QUANTILE],
        )
        for hid, flags in cur.fetchall():
            result[int(hid)] = int(flags)
    return result


def _delete_from_anomalies(conn, hylak_ids: list[int]) -> None:
    if not hylak_ids:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM area_anomalies WHERE hylak_id = ANY(%s)", [hylak_ids])
    conn.commit()


def _update_flags(conn, updates: list[tuple[int, int]]) -> None:
    if not updates:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE area_anomalies SET anomaly_flags = %s WHERE hylak_id = %s",
            [(flags, hid) for hid, flags in updates],
        )
    conn.commit()


def main() -> None:
    Logger("recheck_zero_quantile")
    args = parse_args()

    zero_quantile_config = ZeroQuantileConfig(quantile=args.zero_quantile)
    filters = default_filters(zero_quantile_config=zero_quantile_config)

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(hylak_id) FROM lake_info")
            row = cur.fetchone()
        max_id = int(row[0]) if row and row[0] is not None else None
        zero_lakes = _load_zero_quantile_lakes(conn)

    if max_id is None:
        log.error("No lakes found in lake_info")
        return

    log.info(
        "Loaded %d zero-quantile flagged lakes, max_hylak_id=%d, quantile=%.2f",
        len(zero_lakes), max_id, args.zero_quantile,
    )

    all_chunks = [
        (start, start + args.chunk_size)
        for start in range(0, max_id + 1, args.chunk_size)
    ]
    total_chunks = len(all_chunks)

    total_checked = 0
    rescued_to_quality = 0
    flags_updated = 0
    still_anomalous = 0
    quantile_zero = 0

    for idx, (chunk_start, chunk_end) in enumerate(all_chunks, 1):
        candidate_ids = {
            hid for hid in zero_lakes
            if chunk_start <= hid < chunk_end
        }
        if not candidate_ids:
            continue

        log.info(
            "[%d/%d] chunk %d-%d: %d candidates",
            idx, total_chunks, chunk_start, chunk_end - 1, len(candidate_ids),
        )

        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            atlas_areas = fetch_atlas_area_chunk(conn, chunk_start, chunk_end)
            frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

        to_quality: list[dict] = []
        flag_updates: list[tuple[int, int]] = []
        delete_from_anomalies: list[int] = []

        for hylak_id, df in lake_frames.items():
            if hylak_id not in candidate_ids:
                continue

            frozen_ym = frozen_map.get(hylak_id, None)
            df_no_frozen = filter_frozen_rows(df, frozen_ym)

            rs_area_median = compute_median_area(df_no_frozen) / 1_000_000
            rs_area_mean = compute_mean_area(df_no_frozen) / 1_000_000
            rs_area_quantile = compute_quantile_area(df_no_frozen, quantile=args.zero_quantile) / 1_000_000
            atlas_area = atlas_areas.get(hylak_id, 0.0)

            total_checked += 1

            if rs_area_quantile == 0.0:
                quantile_zero += 1
                continue

            ctx = LakeContext(
                df=df,
                df_no_frozen=df_no_frozen,
                rs_area_median=rs_area_median,
                rs_area_mean=rs_area_mean,
                rs_area_quantile=rs_area_quantile,
                atlas_area=atlas_area,
            )

            decision = classify_area_anomaly(ctx, filters)
            old_flags = zero_lakes[hylak_id]

            if not decision["is_anomalous"]:
                row = {
                    "hylak_id": hylak_id,
                    "rs_area_mean": rs_area_mean,
                    "rs_area_median": rs_area_median,
                    "atlas_area": atlas_area,
                }
                to_quality.append(row)
                delete_from_anomalies.append(hylak_id)
                rescued_to_quality += 1
            else:
                new_flags = int(decision["anomaly_flags"])
                if new_flags != old_flags:
                    flag_updates.append((hylak_id, new_flags))
                    flags_updated += 1
                else:
                    still_anomalous += 1

        n_changes = len(to_quality) + len(flag_updates)
        if n_changes > 0:
            log.info(
                "[%d/%d] chunk %d-%d: rescued=%d, flags_updated=%d, still_anomalous=%d",
                idx, total_chunks, chunk_start, chunk_end - 1,
                len(to_quality), len(flag_updates), still_anomalous,
            )

        if not args.dry_run and n_changes > 0:
            with series_db.connection_context() as conn:
                _delete_from_anomalies(conn, delete_from_anomalies)
                if to_quality:
                    upsert_area_quality(conn, to_quality)
                _update_flags(conn, flag_updates)

    print(f"\n=== Recheck Zero-Quantile Summary (quantile={args.zero_quantile}) ===")
    print(f"Total zero-quantile flagged lakes: {len(zero_lakes)}")
    print(f"Lakes checked:                    {total_checked}")
    print(f"  Quantile still zero:            {quantile_zero}")
    print(f"  Quantile now nonzero:           {total_checked - quantile_zero}")
    print(f"    Rescued to area_quality:       {rescued_to_quality}")
    print(f"    Flags updated:                 {flags_updated}")
    print(f"    Still anomalous (same flags):  {still_anomalous}")
    if args.dry_run:
        print("\n[DRY RUN - no changes written]")


if __name__ == "__main__":
    main()
