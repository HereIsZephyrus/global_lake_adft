"""Chunked DB batch runner for PWM extreme quantile workflow."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Callable

import pandas as pd

from lakesource.postgres import fetch_lake_area_chunk, series_db
from lakesource.pwm_extreme.schema import (
    PWMExtremeBatchConfig,
    PWMExtremeResult,
    PWMExtremeServiceConfig,
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.pwm_extreme.store import (
    ensure_pwm_extreme_tables,
    make_run_status_row,
    result_to_threshold_rows,
    upsert_pwm_extreme_run_status,
    upsert_pwm_extreme_thresholds,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChunkProcessPayload:
    """DB-ready rows and counters produced by one chunk processing step."""

    threshold_rows: list[dict]
    status_rows: list[dict]
    skipped_lakes: int
    success_lakes: int
    error_lakes: int


@dataclass(frozen=True)
class BatchRunReport:
    """Execution report for a batch run."""

    total_chunks: int
    processed_chunks: int
    skipped_chunks: int
    source_lakes: int
    skipped_lakes: int
    success_lakes: int
    error_lakes: int


def process_chunk_lakes(
    lake_map: dict[int, pd.DataFrame],
    *,
    chunk_start: int,
    chunk_end: int,
    workflow_version: str,
    service_config: PWMExtremeServiceConfig,
    processed_hylak_ids: set[int] | None = None,
    run_single_fn: Callable[..., PWMExtremeResult] | None = None,
) -> ChunkProcessPayload:
    """Process all lakes in a chunk and isolate per-lake failures."""
    if run_single_fn is None:
        from .service import run_single_lake_service
        run_single_fn = run_single_lake_service

    done_ids = processed_hylak_ids or set()

    threshold_rows: list[dict] = []
    status_rows: list[dict] = []
    skipped_lakes = 0
    success_lakes = 0
    error_lakes = 0

    for hylak_id in sorted(lake_map):
        if hylak_id in done_ids:
            skipped_lakes += 1
            continue
        try:
            result = run_single_fn(
                lake_map[hylak_id],
                hylak_id=hylak_id,
                config=service_config,
                frozen_year_months=None,
                use_frozen_mask=False,
            )
        except Exception as exc:
            error_lakes += 1
            status_rows.append(
                make_run_status_row(
                    hylak_id=hylak_id,
                    chunk_start=chunk_start,
                    chunk_end=chunk_end,
                    workflow_version=workflow_version,
                    status=RUN_STATUS_ERROR,
                    error_message=str(exc),
                )
            )
            continue

        success_lakes += 1
        threshold_rows.extend(
            result_to_threshold_rows(result, workflow_version=workflow_version)
        )
        status_rows.append(
            make_run_status_row(
                hylak_id=hylak_id,
                chunk_start=chunk_start,
                chunk_end=chunk_end,
                workflow_version=workflow_version,
                status=RUN_STATUS_DONE,
                error_message=None,
            )
        )

    return ChunkProcessPayload(
        threshold_rows=threshold_rows,
        status_rows=status_rows,
        skipped_lakes=skipped_lakes,
        success_lakes=success_lakes,
        error_lakes=error_lakes,
    )


def _iter_chunk_ranges(
    max_hylak_id: int, chunk_size: int, limit_id: int | None
) -> list[tuple[int, int]]:
    upper_bound = max_hylak_id
    if limit_id is not None:
        upper_bound = min(upper_bound, limit_id - 1)
    if upper_bound < 0:
        return []
    ranges: list[tuple[int, int]] = []
    for chunk_start in range(0, upper_bound + 1, chunk_size):
        chunk_end = chunk_start + chunk_size
        if limit_id is not None:
            chunk_end = min(chunk_end, limit_id)
        ranges.append((chunk_start, chunk_end))
    return ranges


def _persist_chunk_payload(payload: ChunkProcessPayload) -> None:
    with series_db.connection_context() as conn:
        try:
            upsert_pwm_extreme_thresholds(conn, payload.threshold_rows, commit=False)
            upsert_pwm_extreme_run_status(conn, payload.status_rows, commit=False)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def run_pwm_extreme_batch(
    config: PWMExtremeBatchConfig,
) -> BatchRunReport:
    """Run chunked batch execution over quality-filtered monthly series."""
    config.output_root.mkdir(parents=True, exist_ok=True)
    service_config = config.service_config

    with series_db.connection_context() as conn:
        ensure_pwm_extreme_tables(conn)

    from lakesource.postgres import fetch_max_area_quality_hylak_id
    with series_db.connection_context() as conn:
        max_hylak_id = fetch_max_area_quality_hylak_id(conn)

    chunk_ranges = _iter_chunk_ranges(max_hylak_id, config.chunk_size, config.limit_id)

    processed_chunks = 0
    skipped_chunks = 0
    source_lakes = 0
    skipped_lakes = 0
    success_lakes = 0
    error_lakes = 0

    for chunk_start, chunk_end in chunk_ranges:
        with series_db.connection_context() as conn:
            lake_map = fetch_lake_area_chunk(conn, chunk_start, chunk_end)

        source_count = len(lake_map)
        source_lakes += source_count
        if source_count == 0:
            skipped_chunks += 1
            continue

        payload = process_chunk_lakes(
            lake_map,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            workflow_version=config.workflow_version,
            service_config=service_config,
        )
        _persist_chunk_payload(payload)
        processed_chunks += 1
        skipped_lakes += payload.skipped_lakes
        success_lakes += payload.success_lakes
        error_lakes += payload.error_lakes
        log.info(
            "Chunk [%d, %d): source=%d skip=%d success=%d error=%d",
            chunk_start,
            chunk_end,
            source_count,
            payload.skipped_lakes,
            payload.success_lakes,
            payload.error_lakes,
        )

    return BatchRunReport(
        total_chunks=len(chunk_ranges),
        processed_chunks=processed_chunks,
        skipped_chunks=skipped_chunks,
        source_lakes=source_lakes,
        skipped_lakes=skipped_lakes,
        success_lakes=success_lakes,
        error_lakes=error_lakes,
    )
