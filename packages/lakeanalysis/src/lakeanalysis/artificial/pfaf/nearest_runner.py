"""Application runner for nearest-natural-lake search."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from lakesource.postgres import series_db

from .nearest import compute_nearest_naturals
from .store import ensure_af_nearest_table, upsert_af_nearest

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class NearestRunConfig:
    limit_id: int | None = None
    max_area_ratio: float = 10.0


def run_nearest(config: NearestRunConfig) -> list[dict]:
    """Execute the nearest-natural-lake pipeline."""
    log.info(
        "Starting nearest-natural pipeline, limit_id=%s, max_area_ratio=%.1f",
        config.limit_id if config.limit_id is not None else "none",
        config.max_area_ratio,
    )

    with series_db.connection_context() as conn:
        ensure_af_nearest_table(conn)
        rows = compute_nearest_naturals(
            conn,
            limit_id=config.limit_id,
            max_area_ratio=config.max_area_ratio,
        )

    matched = sum(1 for row in rows if row["nearest_id"] is not None)
    log.info(
        "Search done: %d/%d type>1 lakes matched a natural lake",
        matched,
        len(rows),
    )

    with series_db.connection_context() as conn:
        log.info("Upserting %d row(s) into af_nearest...", len(rows))
        upsert_af_nearest(conn, rows)

    log.info("Pipeline complete.")
    return rows
