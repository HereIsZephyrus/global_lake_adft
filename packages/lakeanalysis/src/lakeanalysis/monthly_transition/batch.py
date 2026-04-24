"""Chunked DB batch runner for monthly transition workflow."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
from typing import Callable

import pandas as pd

from lakesource.postgres import fetch_lake_area_chunk, series_db
from lakesource.monthly_transition.schema import (
    MonthlyTransitionBatchConfig,
    MonthlyTransitionResult,
    MonthlyTransitionServiceConfig,
    RUN_STATUS_DONE,
    RUN_STATUS_ERROR,
)
from lakesource.monthly_transition.store import (
    ensure_monthly_transition_tables,
    fetch_summary_cache_sources,
    fetch_max_hylak_id,
    fetch_processed_hylak_ids_in_chunk,
    fetch_source_hylak_ids_in_chunk,
    make_run_status_row,
    result_to_extreme_rows,
    result_to_label_rows,
    result_to_transition_rows,
    upsert_monthly_transition_abrupt_transitions,
    upsert_monthly_transition_extremes,
    upsert_monthly_transition_labels,
    upsert_monthly_transition_run_status,
)
from .summary import (
    SummaryAccumulator,
    cache_root_for,
    save_summary_plots_from_cache,
    write_summary_cache,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChunkProcessPayload:
    """DB-ready rows and counters produced by one chunk processing step."""

    label_rows: list[dict]
    extreme_rows: list[dict]
    transition_rows: list[dict]
    status_rows: list[dict]
    skipped_lakes: int
    success_lakes: int
    error_lakes: int
    summary: SummaryAccumulator


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
    cache_paths: dict[str, Path] | None
    plot_paths: dict[str, Path] | None


def process_chunk_lakes(
    lake_map: dict[int, pd.DataFrame],
    *,
    chunk_start: int,
    chunk_end: int,
    workflow_version: str,
    service_config: MonthlyTransitionServiceConfig,
    processed_hylak_ids: set[int] | None = None,
    run_single_fn: Callable[..., MonthlyTransitionResult] | None = None,
) -> ChunkProcessPayload:
    """Process all lakes in a chunk and isolate per-lake failures.

    Args:
        run_single_fn: Callable that runs one lake through the workflow.
            If None, defaults to ``run_single_lake_service`` from this package
            (lazy import to avoid hard-wiring the dependency).
    """
    if run_single_fn is None:
        from .service import run_single_lake_service
        run_single_fn = run_single_lake_service

    done_ids = processed_hylak_ids or set()

    label_rows: list[dict] = []
    extreme_rows: list[dict] = []
    transition_rows: list[dict] = []
    status_rows: list[dict] = []
    skipped_lakes = 0
    success_lakes = 0
    error_lakes = 0
    summary = SummaryAccumulator()

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
        except Exception as exc:  # pylint: disable=broad-except
            error_lakes += 1
            summary.update_error()
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
        summary.update_success(result)
        label_rows.extend(
            result_to_label_rows(result, workflow_version=workflow_version)
        )
        extreme_rows.extend(
            result_to_extreme_rows(result, workflow_version=workflow_version)
        )
        transition_rows.extend(
            result_to_transition_rows(result, workflow_version=workflow_version)
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
        label_rows=label_rows,
        extreme_rows=extreme_rows,
        transition_rows=transition_rows,
        status_rows=status_rows,
        skipped_lakes=skipped_lakes,
        success_lakes=success_lakes,
        error_lakes=error_lakes,
        summary=summary,
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
            upsert_monthly_transition_labels(conn, payload.label_rows, commit=False)
            upsert_monthly_transition_extremes(conn, payload.extreme_rows, commit=False)
            upsert_monthly_transition_abrupt_transitions(
                conn, payload.transition_rows, commit=False
            )
            upsert_monthly_transition_run_status(
                conn, payload.status_rows, commit=False
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def run_monthly_transition_batch(
    config: MonthlyTransitionBatchConfig,
) -> BatchRunReport:
    """Run chunked batch execution over quality-filtered monthly series."""
    config.output_root.mkdir(parents=True, exist_ok=True)
    service_config = MonthlyTransitionServiceConfig(
        min_valid_per_month=config.min_valid_per_month,
        min_valid_observations=config.min_valid_observations,
    )

    with series_db.connection_context() as conn:
        ensure_monthly_transition_tables(conn)
        max_hylak_id = fetch_max_hylak_id(conn)
    chunk_ranges = _iter_chunk_ranges(max_hylak_id, config.chunk_size, config.limit_id)

    processed_chunks = 0
    skipped_chunks = 0
    source_lakes = 0
    skipped_lakes = 0
    success_lakes = 0
    error_lakes = 0
    for chunk_start, chunk_end in chunk_ranges:
        with series_db.connection_context() as conn:
            source_ids = fetch_source_hylak_ids_in_chunk(conn, chunk_start, chunk_end)
            processed_ids = fetch_processed_hylak_ids_in_chunk(
                conn,
                chunk_start,
                chunk_end,
                workflow_version=config.workflow_version,
            )
        source_count = len(source_ids)
        source_lakes += source_count
        if source_count == 0:
            skipped_chunks += 1
            continue
        if source_ids.issubset(processed_ids):
            skipped_chunks += 1
            continue

        with series_db.connection_context() as conn:
            lake_map = fetch_lake_area_chunk(conn, chunk_start, chunk_end)

        payload = process_chunk_lakes(
            lake_map,
            chunk_start=chunk_start,
            chunk_end=chunk_end,
            workflow_version=config.workflow_version,
            service_config=service_config,
            processed_hylak_ids=processed_ids,
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

    cache_paths: dict[str, Path] | None = None
    plot_paths: dict[str, Path] | None = None
    if config.build_summary_cache:
        cache_root = cache_root_for(config.output_root)
        with series_db.connection_context() as conn:
            cache_payload = fetch_summary_cache_sources(
                conn,
                workflow_version=config.workflow_version,
            )
        cache_paths = write_summary_cache(
            cache_root,
            **cache_payload,
        )
        if config.plot_summary:
            plot_paths = save_summary_plots_from_cache(
                cache_root=cache_root,
                output_root=config.output_root / "summary",
            )

    return BatchRunReport(
        total_chunks=len(chunk_ranges),
        processed_chunks=processed_chunks,
        skipped_chunks=skipped_chunks,
        source_lakes=source_lakes,
        skipped_lakes=skipped_lakes,
        success_lakes=success_lakes,
        error_lakes=error_lakes,
        cache_paths=cache_paths,
        plot_paths=plot_paths,
    )
