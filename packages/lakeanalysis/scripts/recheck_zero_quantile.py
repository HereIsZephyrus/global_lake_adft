"""Recheck lakes flagged by median_zero using a higher quantile threshold.

For lakes currently in area_anomalies with the zero_quantile (bit 1) flag,
recompute the quantile area at the new threshold.  If the new quantile is
non-zero, re-run all filters and move lakes that are no longer anomalous
to area_quality, or update their anomaly_flags if other filters still
trigger.

Two-phase approach for speed:
  Phase 1: SQL-side PERCENTILE_CONT to find lakes where the new quantile
           is non-zero (skips ~60% of candidates without fetching raw data).
  Phase 2: Fetch full time-series only for those lakes, run all filters.

Usage:
    # Dry run (stats only):
    uv run python scripts/recheck_zero_quantile.py --dry-run

    # Execute with default p80:
    uv run python scripts/recheck_zero_quantile.py

    # Use p90 instead:
    uv run python scripts/recheck_zero_quantile.py --zero-quantile 0.90

    # Custom batch size:
    uv run python scripts/recheck_zero_quantile.py --batch-size 10000
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict

import pandas as pd

from lakesource.postgres import series_db, upsert_area_quality
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
    parser.add_argument("--batch-size", type=int, default=10_000, metavar="N")
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


def _find_nonzero_quantile_lakes(
    conn, hylak_ids: list[int], quantile: float,
) -> set[int]:
    """Phase 1: SQL-side quantile computation to find lakes where quantile > 0."""
    ph = ",".join(["%s"] * len(hylak_ids))
    result: set[int] = set()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT la.hylak_id
            FROM lake_area la
            WHERE la.hylak_id IN ({ph})
              AND NOT EXISTS (
                SELECT 1 FROM anomaly a
                WHERE a.hylak_id = la.hylak_id
                  AND a.anomaly_type = 'frozen'
                  AND a.year_month = la.year_month
              )
            GROUP BY la.hylak_id
            HAVING PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY la.water_area) > 0
        """, hylak_ids + [quantile])
        for (hid,) in cur.fetchall():
            result.add(int(hid))
    return result


def _fetch_full_data(
    conn,
    hylak_ids: list[int],
) -> tuple[dict[int, pd.DataFrame], dict[int, float], dict[int, list[int]]]:
    """Phase 2: Fetch full time-series for lakes that passed phase 1."""
    ph = ",".join(["%s"] * len(hylak_ids))

    lake_frames: dict[int, pd.DataFrame] = {}
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT hylak_id, year_month, water_area
            FROM lake_area
            WHERE hylak_id IN ({ph})
            ORDER BY hylak_id, year_month
        """, hylak_ids)
        rows = cur.fetchall()

    by_lake: dict[int, list] = defaultdict(list)
    for hid, ym, area in rows:
        by_lake[hid].append((ym, area))
    for hid, records in by_lake.items():
        lake_frames[hid] = pd.DataFrame(records, columns=["year_month", "water_area"])

    atlas_areas: dict[int, float] = {}
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT hylak_id, lake_area FROM lake_info WHERE hylak_id IN ({ph})
        """, hylak_ids)
        for hid, area in cur.fetchall():
            atlas_areas[int(hid)] = float(area)

    frozen_map: dict[int, list[int]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT hylak_id,
                   (EXTRACT(YEAR FROM year_month)::int * 100
                    + EXTRACT(MONTH FROM year_month)::int) AS year_month_key
            FROM anomaly
            WHERE hylak_id IN ({ph}) AND anomaly_type = 'frozen'
            ORDER BY hylak_id, year_month
        """, hylak_ids)
        for hid, ym_key in cur.fetchall():
            frozen_map[int(hid)].append(int(ym_key))

    return lake_frames, atlas_areas, frozen_map


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
        zero_lakes = _load_zero_quantile_lakes(conn)

    if not zero_lakes:
        log.info("No zero-quantile flagged lakes found.")
        return

    candidate_ids = sorted(zero_lakes.keys())
    log.info(
        "Loaded %d zero-quantile flagged lakes, quantile=%.2f",
        len(candidate_ids), args.zero_quantile,
    )

    batches = [
        candidate_ids[i : i + args.batch_size]
        for i in range(0, len(candidate_ids), args.batch_size)
    ]
    total_batches = len(batches)

    total_checked = 0
    rescued_to_quality = 0
    flags_updated = 0
    still_anomalous = 0
    quantile_zero = 0

    for idx, batch_ids in enumerate(batches, 1):
        log.info(
            "[%d/%d] phase 1: SQL quantile filter on %d lakes",
            idx, total_batches, len(batch_ids),
        )

        with series_db.connection_context() as conn:
            nonzero_ids = _find_nonzero_quantile_lakes(conn, batch_ids, args.zero_quantile)

        quantile_zero_in_batch = len(batch_ids) - len(nonzero_ids)
        quantile_zero += quantile_zero_in_batch
        total_checked += len(batch_ids)

        log.info(
            "[%d/%d] phase 1 done: %d quantile>0, %d quantile=0",
            idx, total_batches, len(nonzero_ids), quantile_zero_in_batch,
        )

        if not nonzero_ids:
            continue

        nonzero_list = sorted(nonzero_ids)
        log.info(
            "[%d/%d] phase 2: fetching full data for %d lakes",
            idx, total_batches, len(nonzero_list),
        )

        with series_db.connection_context() as conn:
            lake_frames, atlas_areas, frozen_map = _fetch_full_data(conn, nonzero_list)

        to_quality: list[dict] = []
        flag_updates: list[tuple[int, int]] = []
        delete_from_anomalies: list[int] = []

        for hylak_id in nonzero_list:
            df = lake_frames.get(hylak_id)
            if df is None or df.empty:
                continue

            frozen_ym = frozen_map.get(hylak_id, None)
            df_no_frozen = filter_frozen_rows(df, frozen_ym)

            rs_area_median = compute_median_area(df_no_frozen) / 1_000_000
            rs_area_mean = compute_mean_area(df_no_frozen) / 1_000_000
            rs_area_quantile = compute_quantile_area(df_no_frozen, quantile=args.zero_quantile) / 1_000_000
            atlas_area = atlas_areas.get(hylak_id, 0.0)

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
        log.info(
            "[%d/%d] phase 2 done: rescued=%d, flags_updated=%d, still_anomalous=%d",
            idx, total_batches,
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
