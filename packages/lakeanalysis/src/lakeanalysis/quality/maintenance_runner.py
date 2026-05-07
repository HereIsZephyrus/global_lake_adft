"""Application runners for quality maintenance workflows."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lakesource.config import SourceConfig
from lakesource.provider.factory import create_provider

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
    batch_size: int = 1_000
    dry_run: bool = False


def _provider(source_config: SourceConfig | None = None):
    return create_provider(source_config or SourceConfig())


def _clear_zero_quantile_flag(conn: object, hylak_ids: list[int]) -> int:
    """Compatibility shim retained for unit tests around the SQL update shape."""

    if not hylak_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE area_anomalies SET anomaly_flags = anomaly_flags & ~%s WHERE hylak_id = ANY(%s)",
            [FLAG_ZERO_QUANTILE, hylak_ids],
        )
        updated = int(cur.rowcount or 0)
    conn.commit()
    return updated


def run_recompute_pv(config: RecomputePvConfig, source_config: SourceConfig | None = None) -> None:
    filters = build_quality_filters(QualityRunConfig())
    pv_filter = filters[4]
    provider = _provider(source_config)

    total_lakes = 0
    pv_triggered = 0
    pv_passed = 0
    moved_q_to_a = 0
    moved_a_to_q = 0
    flags_cleared = 0

    max_id = provider.fetch_max_hylak_id()
    all_status = provider.fetch_area_statuses()

    if max_id <= 0:
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

        lake_frames = provider.fetch_lake_area_chunk(chunk_start, chunk_end)
        atlas_areas = provider.fetch_atlas_area_chunk(chunk_start, chunk_end)
        frozen_map = provider.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

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
                frozen_year_months=frozenset(frozen_map.get(hylak_id, set())),
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
            provider.delete_ids("area_quality", delete_from_quality)
            provider.delete_ids("area_anomalies", delete_from_anomalies)
            if to_quality:
                provider.upsert_rows("area_quality", to_quality)
            if to_anomalies:
                provider.upsert_rows("area_anomalies", to_anomalies)
            provider.update_area_anomaly_flags(flag_updates)

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


def run_recheck_zero_quantile(
    config: RecheckZeroQuantileConfig, source_config: SourceConfig | None = None
) -> None:
    provider = _provider(source_config)
    zero_lakes = provider.fetch_zero_quantile_flags()

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
    flags_updated = 0
    quantile_zero = 0

    for idx, batch_ids in enumerate(batches, 1):
        log.info("[%d/%d] phase 1: SQL quantile filter on %d lakes", idx, total_batches, len(batch_ids))
        nonzero_ids = provider.find_nonzero_quantile_lakes(batch_ids, config.zero_quantile)

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
        log.info("[%d/%d] phase 2: clearing bit1 for %d lakes", idx, total_batches, len(nonzero_list))

        batch_updated = len(nonzero_list)
        if not config.dry_run:
            batch_updated = provider.clear_zero_quantile_flag(nonzero_list)
        flags_updated += batch_updated

        log.info(
            "[%d/%d] phase 2 done: flags_updated=%d",
            idx,
            total_batches,
            batch_updated,
        )

    print(f"\n=== Recheck Zero-Quantile Summary (quantile={config.zero_quantile}) ===")
    print(f"Total zero-quantile flagged lakes: {len(zero_lakes)}")
    print(f"Lakes checked:                    {total_checked}")
    print(f"  Quantile still zero:            {quantile_zero}")
    print(f"  Quantile now nonzero:           {total_checked - quantile_zero}")
    print(f"    Flags updated in anomalies:    {flags_updated}")
    print(f"    Still zero-quantile flagged:   {quantile_zero}")
    if config.dry_run:
        print("\n[DRY RUN - no changes written]")
