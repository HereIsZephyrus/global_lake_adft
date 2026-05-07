"""Application runners for quality maintenance workflows."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass

import pandas as pd

from lakesource.postgres import (
    fetch_atlas_area_chunk,
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    series_db,
    upsert_area_anomalies,
    upsert_area_quality,
)

from . import FLAG_ZERO_QUANTILE, QualityRunConfig, classify_area_anomaly
from .runner import build_quality_context, build_quality_filters

log = logging.getLogger(__name__)

FLAG_PV = 16


@dataclass(frozen=True)
class RecomputePvConfig:
    chunk_size: int = 5_000
    start_id: int = 0
    limit_id: int | None = None
    dry_run: bool = False


@dataclass(frozen=True)
class RecheckZeroQuantileConfig:
    zero_quantile: float = 0.80
    batch_size: int = 10_000
    dry_run: bool = False


def _load_all_status(conn: object) -> dict[int, tuple[str, int]]:
    result: dict[int, tuple[str, int]] = {}
    with conn.cursor() as cur:
        cur.execute("SELECT hylak_id FROM area_quality")
        for (hid,) in cur.fetchall():
            result[int(hid)] = ("quality", 0)
        cur.execute("SELECT hylak_id, anomaly_flags FROM area_anomalies")
        for hid, flags in cur.fetchall():
            result[int(hid)] = ("anomalies", int(flags))
    return result


def _delete_from_quality(conn: object, hylak_ids: list[int]) -> None:
    if not hylak_ids:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM area_quality WHERE hylak_id = ANY(%s)", [hylak_ids])
    conn.commit()


def _delete_from_anomalies(conn: object, hylak_ids: list[int]) -> None:
    if not hylak_ids:
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM area_anomalies WHERE hylak_id = ANY(%s)", [hylak_ids])
    conn.commit()


def _update_flags(conn: object, updates: list[tuple[int, int]]) -> None:
    if not updates:
        return
    with conn.cursor() as cur:
        cur.executemany(
            "UPDATE area_anomalies SET anomaly_flags = %s WHERE hylak_id = %s",
            [(flags, hid) for hid, flags in updates],
        )
    conn.commit()


def _fetch_zero_quantile_lakes(conn: object) -> dict[int, int]:
    result: dict[int, int] = {}
    with conn.cursor() as cur:
        cur.execute(
            "SELECT hylak_id, anomaly_flags FROM area_anomalies WHERE anomaly_flags & %s > 0",
            [FLAG_ZERO_QUANTILE],
        )
        for hid, flags in cur.fetchall():
            result[int(hid)] = int(flags)
    return result


def _find_nonzero_quantile_lakes(conn: object, hylak_ids: list[int], quantile: float) -> set[int]:
    placeholders = ",".join(["%s"] * len(hylak_ids))
    result: set[int] = set()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT la.hylak_id
            FROM lake_area la
            WHERE la.hylak_id IN ({placeholders})
              AND NOT EXISTS (
                SELECT 1 FROM anomaly a
                WHERE a.hylak_id = la.hylak_id
                  AND a.anomaly_type = 'frozen'
                  AND a.year_month = la.year_month
              )
            GROUP BY la.hylak_id
            HAVING PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY la.water_area) > 0
            """,
            hylak_ids + [quantile],
        )
        for (hid,) in cur.fetchall():
            result.add(int(hid))
    return result


def _fetch_full_data(
    conn: object,
    hylak_ids: list[int],
) -> tuple[dict[int, pd.DataFrame], dict[int, float], dict[int, list[int]]]:
    placeholders = ",".join(["%s"] * len(hylak_ids))

    lake_frames: dict[int, pd.DataFrame] = {}
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT hylak_id, year_month, water_area
            FROM lake_area
            WHERE hylak_id IN ({placeholders})
            ORDER BY hylak_id, year_month
            """,
            hylak_ids,
        )
        rows = cur.fetchall()

    by_lake: dict[int, list[tuple[object, object]]] = defaultdict(list)
    for hid, ym, area in rows:
        by_lake[int(hid)].append((ym, area))
    for hid, records in by_lake.items():
        frame = pd.DataFrame(records, columns=["year_month", "water_area"])
        year_month = pd.to_datetime(frame["year_month"])
        frame["year"] = year_month.dt.year.astype(int)
        frame["month"] = year_month.dt.month.astype(int)
        lake_frames[hid] = frame[["year", "month", "water_area", "year_month"]]

    atlas_areas: dict[int, float] = {}
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT hylak_id, lake_area FROM lake_info WHERE hylak_id IN ({placeholders})",
            hylak_ids,
        )
        for hid, area in cur.fetchall():
            atlas_areas[int(hid)] = float(area)

    frozen_map: dict[int, list[int]] = defaultdict(list)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT hylak_id,
                   (EXTRACT(YEAR FROM year_month)::int * 100
                    + EXTRACT(MONTH FROM year_month)::int) AS year_month_key
            FROM anomaly
            WHERE hylak_id IN ({placeholders}) AND anomaly_type = 'frozen'
            ORDER BY hylak_id, year_month
            """,
            hylak_ids,
        )
        for hid, ym_key in cur.fetchall():
            frozen_map[int(hid)].append(int(ym_key))

    return lake_frames, atlas_areas, frozen_map


def run_recompute_pv(config: RecomputePvConfig) -> None:
    filters = build_quality_filters(QualityRunConfig())
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

    all_chunks = [(start, start + config.chunk_size) for start in range(0, max_id + 1, config.chunk_size)]
    if config.start_id > 0:
        all_chunks = [(s, e) for s, e in all_chunks if s >= config.start_id]
    if config.limit_id is not None and all_chunks:
        all_chunks = [(s, e) for s, e in all_chunks if s < config.limit_id]
        all_chunks[-1] = (all_chunks[-1][0], min(all_chunks[-1][1], config.limit_id))
    total_chunks = len(all_chunks)

    for idx, (chunk_start, chunk_end) in enumerate(all_chunks, 1):
        log.info("[%d/%d] chunk %d-%d: processing...", idx, total_chunks, chunk_start, chunk_end - 1)

        with series_db.connection_context() as conn:
            lake_frames = fetch_lake_area_chunk(conn, chunk_start, chunk_end)
            atlas_areas = fetch_atlas_area_chunk(conn, chunk_start, chunk_end)
            frozen_map = fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

        to_quality: list[dict[str, int | float]] = []
        to_anomalies: list[dict[str, int | float]] = []
        flag_updates: list[tuple[int, int]] = []
        delete_from_quality: list[int] = []
        delete_from_anomalies: list[int] = []

        for hylak_id, df in lake_frames.items():
            if hylak_id not in all_status:
                continue

            ctx, metrics = build_quality_context(
                df=df,
                atlas_area=atlas_areas.get(hylak_id, 0.0),
                frozen_ym=frozen_map.get(hylak_id),
                zero_quantile=0.80,
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

            row = {"hylak_id": hylak_id, **metrics}
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
            idx,
            total_chunks,
            chunk_start,
            chunk_end - 1,
            len(lake_frames),
            len(to_anomalies),
            len(to_quality),
            len(flag_updates),
        )

        if not config.dry_run and n_moves > 0:
            with series_db.connection_context() as conn:
                _delete_from_quality(conn, delete_from_quality)
                _delete_from_anomalies(conn, delete_from_anomalies)
                if to_quality:
                    upsert_area_quality(conn, to_quality)
                if to_anomalies:
                    upsert_area_anomalies(conn, to_anomalies)
                _update_flags(conn, flag_updates)

    print("\n=== Recompute PV Summary ===")
    print(f"Total lakes checked:  {total_lakes}")
    if total_lakes > 0:
        print(f"PV triggered:         {pv_triggered} ({100 * pv_triggered / total_lakes:.1f}%)")
        print(f"PV passed:            {pv_passed} ({100 * pv_passed / total_lakes:.1f}%)")
    print(f"Moved quality→anomalies: {moved_q_to_a}")
    print(f"Moved anomalies→quality: {moved_a_to_q}")
    print(f"Flags updated (bit 5 cleared): {flags_cleared}")
    if config.dry_run:
        print("\n[DRY RUN - no changes written]")


def run_recheck_zero_quantile(config: RecheckZeroQuantileConfig) -> None:
    zero_quantile_config = QualityRunConfig(zero_quantile=config.zero_quantile)
    filters = build_quality_filters(zero_quantile_config)

    with series_db.connection_context() as conn:
        zero_lakes = _fetch_zero_quantile_lakes(conn)

    if not zero_lakes:
        log.info("No zero-quantile flagged lakes found.")
        return

    candidate_ids = sorted(zero_lakes.keys())
    log.info(
        "Loaded %d zero-quantile flagged lakes, quantile=%.2f",
        len(candidate_ids),
        config.zero_quantile,
    )

    batches = [candidate_ids[i : i + config.batch_size] for i in range(0, len(candidate_ids), config.batch_size)]
    total_batches = len(batches)

    total_checked = 0
    rescued_to_quality = 0
    flags_updated = 0
    still_anomalous = 0
    quantile_zero = 0

    for idx, batch_ids in enumerate(batches, 1):
        log.info("[%d/%d] phase 1: SQL quantile filter on %d lakes", idx, total_batches, len(batch_ids))
        with series_db.connection_context() as conn:
            nonzero_ids = _find_nonzero_quantile_lakes(conn, batch_ids, config.zero_quantile)

        quantile_zero_in_batch = len(batch_ids) - len(nonzero_ids)
        quantile_zero += quantile_zero_in_batch
        total_checked += len(batch_ids)

        log.info(
            "[%d/%d] phase 1 done: %d quantile>0, %d quantile=0",
            idx,
            total_batches,
            len(nonzero_ids),
            quantile_zero_in_batch,
        )
        if not nonzero_ids:
            continue

        nonzero_list = sorted(nonzero_ids)
        log.info("[%d/%d] phase 2: fetching full data for %d lakes", idx, total_batches, len(nonzero_list))
        with series_db.connection_context() as conn:
            lake_frames, atlas_areas, frozen_map = _fetch_full_data(conn, nonzero_list)

        to_quality: list[dict[str, int | float]] = []
        flag_updates: list[tuple[int, int]] = []
        delete_from_anomalies: list[int] = []

        for hylak_id in nonzero_list:
            df = lake_frames.get(hylak_id)
            if df is None or df.empty:
                continue

            ctx, metrics = build_quality_context(
                df=df,
                atlas_area=atlas_areas.get(hylak_id, 0.0),
                frozen_ym=frozen_map.get(hylak_id),
                zero_quantile=config.zero_quantile,
            )
            decision = classify_area_anomaly(ctx, filters)
            old_flags = zero_lakes[hylak_id]

            if not bool(decision["is_anomalous"]):
                to_quality.append({"hylak_id": hylak_id, **metrics})
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
            idx,
            total_batches,
            len(to_quality),
            len(flag_updates),
            still_anomalous,
        )

        if not config.dry_run and n_changes > 0:
            with series_db.connection_context() as conn:
                _delete_from_anomalies(conn, delete_from_anomalies)
                if to_quality:
                    upsert_area_quality(conn, to_quality)
                _update_flags(conn, flag_updates)

    print(f"\n=== Recheck Zero-Quantile Summary (quantile={config.zero_quantile}) ===")
    print(f"Total zero-quantile flagged lakes: {len(zero_lakes)}")
    print(f"Lakes checked:                    {total_checked}")
    print(f"  Quantile still zero:            {quantile_zero}")
    print(f"  Quantile now nonzero:           {total_checked - quantile_zero}")
    print(f"    Rescued to area_quality:       {rescued_to_quality}")
    print(f"    Flags updated:                 {flags_updated}")
    print(f"    Still anomalous (same flags):  {still_anomalous}")
    if config.dry_run:
        print("\n[DRY RUN - no changes written]")
