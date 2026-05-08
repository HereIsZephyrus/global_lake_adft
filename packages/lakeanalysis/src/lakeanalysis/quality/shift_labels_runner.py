"""Application runners for shift labels pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from . import FLAG_SHIFT

log = logging.getLogger(__name__)


def upsert_shift_labels_from_parquet(
    parquet_path: str | Path,
    provider: object,
    *,
    chunk_size: int = 10_000,
) -> int:
    """Read area_shift_labels from parquet and upsert to provider."""
    df = pd.read_parquet(parquet_path)
    if df.empty:
        log.info("No rows in %s", parquet_path)
        return 0

    total = 0
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i : i + chunk_size]
        rows = chunk.to_dict("records")
        provider.upsert_rows("area_shift_labels", rows)
        total += len(rows)
        log.info("Upserted %d rows (total %d)", len(rows), total)

    log.info("upsert_shift_labels_from_parquet complete: %d rows", total)
    return total


def sync_shift_to_anomalies(
    provider: object,
    *,
    dry_run: bool = False,
) -> None:
    """Sync area_shift_labels to area_quality / area_anomalies.

    Rules:
    - label='degraded': move lake to area_anomalies with FLAG_SHIFT,
      or add FLAG_SHIFT if already in anomalies without it
    - label='intermittent'/'stable' with only FLAG_SHIFT: move back to area_quality
    """

    labels_rows = provider.fetch_rows("area_shift_labels", 0, 2_000_000_000)  # type: ignore[attr-defined]
    if not labels_rows:
        log.warning("area_shift_labels table is empty, nothing to sync")
        return
    labels_df = pd.DataFrame(labels_rows)

    all_status = provider.fetch_area_statuses()
    log.info("Loaded %d labels and %d status entries", len(labels_df), len(all_status))

    degraded_ids = set(
        labels_df.loc[labels_df["shift_label"] == "degraded", "hylak_id"].astype(int)
    )
    stable_intermittent_ids = set(
        labels_df.loc[labels_df["shift_label"].isin(["stable", "intermittent"]), "hylak_id"].astype(int)
    )

    to_quality: list[dict] = []
    to_anomalies: list[dict] = []
    flag_updates: list[tuple[int, int]] = []
    delete_from_quality: list[int] = []
    delete_from_anomalies: list[int] = []

    processed: set[int] = set()

    for hylak_id in degraded_ids:
        if hylak_id not in all_status or hylak_id in processed:
            continue
        table, flags = all_status[hylak_id]
        processed.add(hylak_id)

        if table == "quality":
            quality_rows = provider.fetch_rows("area_quality", hylak_id, hylak_id + 1)  # type: ignore[attr-defined]
            if quality_rows:
                row = quality_rows[0]
                to_anomalies.append({
                    "hylak_id": hylak_id,
                    "rs_area_mean": row.get("rs_area_mean"),
                    "rs_area_median": row.get("rs_area_median"),
                    "atlas_area": row.get("atlas_area"),
                    "anomaly_flags": FLAG_SHIFT,
                })
                delete_from_quality.append(hylak_id)
                log.debug("degraded %d: quality -> anomalies (add FLAG_SHIFT)", hylak_id)

        elif table == "anomalies":
            if (flags & FLAG_SHIFT) == 0:
                flag_updates.append((hylak_id, flags | FLAG_SHIFT))
                log.debug("degraded %d: anomalies add FLAG_SHIFT", hylak_id)
            else:
                log.debug("degraded %d: anomalies already has FLAG_SHIFT, skip", hylak_id)

    for hylak_id in stable_intermittent_ids:
        if hylak_id not in all_status or hylak_id in processed:
            continue
        table, flags = all_status[hylak_id]
        processed.add(hylak_id)

        if table == "anomalies" and flags == FLAG_SHIFT:
            anomaly_rows = provider.fetch_rows("area_anomalies", hylak_id, hylak_id + 1)  # type: ignore[attr-defined]
            if anomaly_rows:
                row = anomaly_rows[0]
                to_quality.append({
                    "hylak_id": hylak_id,
                    "rs_area_mean": row.get("rs_area_mean"),
                    "rs_area_median": row.get("rs_area_median"),
                    "atlas_area": row.get("atlas_area"),
                })
                delete_from_anomalies.append(hylak_id)
                log.debug("stable/intermittent %d: anomalies -> quality (only FLAG_SHIFT)", hylak_id)

    n_moves = len(to_quality) + len(to_anomalies) + len(flag_updates)
    log.info(
        "Sync summary: quality->anomalies=%d, anomalies->quality=%d, flag_updates=%d",
        len(to_anomalies),
        len(to_quality),
        len(flag_updates),
    )

    if dry_run:
        log.info("[DRY RUN - no changes written]")
        return

    if n_moves > 0:
        provider.delete_ids("area_quality", delete_from_quality)
        provider.delete_ids("area_anomalies", delete_from_anomalies)
        if to_quality:
            provider.upsert_rows("area_quality", to_quality)
        if to_anomalies:
            provider.upsert_rows("area_anomalies", to_anomalies)
        if flag_updates:
            provider.update_area_anomaly_flags(flag_updates)

    print("\n=== Sync Shift Labels Summary ===")
    print(f"Degraded lakes processed:   {len(degraded_ids)}")
    print(f"Stable/intermittent:       {len(stable_intermittent_ids)}")
    print(f"Moved quality->anomalies:  {len(to_anomalies)}")
    print(f"Moved anomalies->quality:  {len(to_quality)}")
    print(f"Flag updates:              {len(flag_updates)}")
