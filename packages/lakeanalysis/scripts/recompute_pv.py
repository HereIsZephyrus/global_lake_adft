"""Recompute PV filter only, moving lakes between area_quality and area_anomalies.

Python-side computation: fetch raw data from PostgreSQL, filter frozen and
compute PV (H×CV) in Python.  Avoids SQL-side query plan instability with
NOT EXISTS / DELETE WHERE EXISTS on the large anomaly table.

Usage:
    # Dry run (stats only):
    uv run python scripts/recompute_pv.py --dry-run

    # Execute:
    uv run python scripts/recompute_pv.py

    # Custom chunk size:
    uv run python scripts/recompute_pv.py --chunk-size 5000

    # Limit to hylak_id < 50000:
    uv run python scripts/recompute_pv.py --dry-run --limit-id 50000
"""

from __future__ import annotations

import argparse
import logging

from lakesource.postgres import (
    fetch_atlas_area_chunk,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_area_anomalies,
    upsert_area_quality,
)
from lakeanalysis.logger import Logger
from lakeanalysis.quality import (
    LakeContext,
    compute_mean_area,
    compute_median_area,
    compute_quantile_area,
    default_filters,
    filter_frozen_rows,
)

log = logging.getLogger(__name__)

FLAG_PV = 16


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute PV filter and update area_quality / area_anomalies.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--chunk-size", type=int, default=5_000, metavar="N")
    parser.add_argument("--start-id", type=int, default=0, metavar="ID", help="Skip chunks with hylak_id < ID.")
    parser.add_argument("--limit-id", type=int, default=None, metavar="ID", help="Only process hylak_id < ID.")
    parser.add_argument("--dry-run", action="store_true", help="Stats only, no writes.")
    return parser.parse_args()


def _load_all_status(conn) -> dict[int, tuple[str, int]]:
    result: dict[int, tuple[str, int]] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT hylak_id FROM area_quality")
        for (hid,) in cur.fetchall():
            result[int(hid)] = ("quality", 0)
        cur.execute("SELECT hylak_id, anomaly_flags FROM area_anomalies")
        for hid, flags in cur.fetchall():
            result[int(hid)] = ("anomalies", int(flags))
    return result


def _delete_from_quality(conn, hylak_ids: list[int]) -> None:
    if not hylak_ids:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM area_quality WHERE hylak_id = ANY(%s)", [hylak_ids])
    conn.commit()


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
    Logger("recompute_pv")
    args = parse_args()

    filters = default_filters()
    pv_filter = filters[4]

    total_lakes = 0
    pv_triggered = 0
    pv_passed = 0
    moved_q_to_a = 0
    moved_a_to_q = 0
    flags_cleared = 0

    with series_db.connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(hylak_id) FROM lake_info")
            row = cur.fetchone()
        max_id = int(row[0]) if row and row[0] is not None else None
        all_status = _load_all_status(conn)

    if max_id is None:
        log.error("No lakes found in lake_info")
        return

    log.info("Loaded status for %d lakes, max_hylak_id=%d", len(all_status), max_id)

    all_chunks = [
        (start, start + args.chunk_size)
        for start in range(0, max_id + 1, args.chunk_size)
    ]
    if args.start_id > 0:
        all_chunks = [(s, e) for s, e in all_chunks if s >= args.start_id]
    if args.limit_id is not None:
        all_chunks = [(s, e) for s, e in all_chunks if s < args.limit_id]
        all_chunks[-1] = (all_chunks[-1][0], min(all_chunks[-1][1], args.limit_id))
    total_chunks = len(all_chunks)

    for idx, (chunk_start, chunk_end) in enumerate(all_chunks, 1):
        log.info("[%d/%d] chunk %d-%d: processing...", idx, total_chunks, chunk_start, chunk_end - 1)

        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            atlas_areas = fetch_atlas_area_chunk(conn, chunk_start, chunk_end)
            frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

        to_quality: list[dict] = []
        to_anomalies: list[dict] = []
        flag_updates: list[tuple[int, int]] = []
        delete_from_quality: list[int] = []
        delete_from_anomalies: list[int] = []

        for hylak_id, df in lake_frames.items():
            if hylak_id not in all_status:
                continue

            frozen_ym = frozen_map.get(hylak_id, None)
            df_no_frozen = filter_frozen_rows(df, frozen_ym)

            rs_area_median = compute_median_area(df_no_frozen) / 1_000_000
            rs_area_mean = compute_mean_area(df_no_frozen) / 1_000_000
            rs_area_quantile = compute_quantile_area(df_no_frozen) / 1_000_000
            atlas_area = atlas_areas.get(hylak_id, 0.0)

            ctx = LakeContext(
                df=df,
                df_no_frozen=df_no_frozen,
                rs_area_median=rs_area_median,
                rs_area_mean=rs_area_mean,
                rs_area_quantile=rs_area_quantile,
                atlas_area=atlas_area,
            )

            pv_flag = pv_filter.classify(ctx)
            new_is_pv = pv_flag.is_anomaly

            total_lakes += 1
            if new_is_pv:
                pv_triggered += 1
            else:
                pv_passed += 1

            table, old_flags = all_status[hylak_id]
            old_is_pv = (old_flags & FLAG_PV) != 0

            if new_is_pv == old_is_pv:
                continue

            row = {
                "hylak_id": hylak_id,
                "rs_area_mean": rs_area_mean,
                "rs_area_median": rs_area_median,
                "atlas_area": atlas_area,
            }

            if table == "quality" and new_is_pv:
                row["anomaly_flags"] = FLAG_PV
                to_anomalies.append(row)
                delete_from_quality.append(hylak_id)
                moved_q_to_a += 1

            elif table == "anomalies" and not new_is_pv:
                other_flags = old_flags & ~FLAG_PV
                if other_flags == 0:
                    to_quality.append(row)
                    delete_from_anomalies.append(hylak_id)
                    moved_a_to_q += 1
                else:
                    flag_updates.append((hylak_id, other_flags))
                    flags_cleared += 1

        n_moves = len(to_quality) + len(to_anomalies) + len(flag_updates)
        log.info(
            "[%d/%d] chunk %d-%d: %d lakes, q→a=%d, a→q=%d, flags=%d",
            idx, total_chunks, chunk_start, chunk_end - 1,
            len(lake_frames),
            len(to_anomalies),
            len(to_quality),
            len(flag_updates),
        )

        if not args.dry_run and n_moves > 0:
            with series_db.connection_context() as conn:
                _delete_from_quality(conn, delete_from_quality)
                _delete_from_anomalies(conn, delete_from_anomalies)
                if to_quality:
                    upsert_area_quality(conn, to_quality)
                if to_anomalies:
                    upsert_area_anomalies(conn, to_anomalies)
                _update_flags(conn, flag_updates)

    print(f"\n=== Recompute PV Summary ===")
    print(f"Total lakes checked:  {total_lakes}")
    if total_lakes > 0:
        print(f"PV triggered:         {pv_triggered} ({100*pv_triggered/total_lakes:.1f}%)")
        print(f"PV passed:            {pv_passed} ({100*pv_passed/total_lakes:.1f}%)")
    print(f"Moved quality→anomalies: {moved_q_to_a}")
    print(f"Moved anomalies→quality: {moved_a_to_q}")
    print(f"Flags updated (bit 5 cleared): {flags_cleared}")
    if args.dry_run:
        print("\n[DRY RUN - no changes written]")


if __name__ == "__main__":
    main()
