"""Shared constants, types, and data classes for the batch framework."""

from __future__ import annotations

from dataclasses import dataclass

TAG_STATUS = 1
TAG_TRIGGER = 2
TAG_DATA = 3

TRIGGER_READ = "trigger_read"
TRIGGER_WRITE = "trigger_write"


class WorkerState:
    PENDING = "pending"
    READING = "reading"
    CALCULATING = "calculating"
    WRITING = "writing"
    DONE = "done"


@dataclass
class RunReport:
    total_chunks: int = 0
    processed_chunks: int = 0
    skipped_chunks: int = 0
    source_lakes: int = 0
    skipped_lakes: int = 0
    success_lakes: int = 0
    error_lakes: int = 0


def _iter_chunk_ranges(
    max_hylak_id: int,
    chunk_size: int,
    start: int = 0,
    end: int | None = None,
) -> list[tuple[int, int]]:
    upper_bound = max_hylak_id
    if end is not None:
        upper_bound = min(upper_bound, end - 1)
    if upper_bound < 0 or upper_bound < start:
        return []
    ranges: list[tuple[int, int]] = []
    for chunk_start in range(start, upper_bound + 1, chunk_size):
        chunk_end = chunk_start + chunk_size
        if end is not None:
            chunk_end = min(chunk_end, end)
        ranges.append((chunk_start, chunk_end))
    return ranges


def _iter_id_batches(
    sorted_ids: list[int],
    batch_size: int,
) -> list[list[int]]:
    if not sorted_ids or batch_size <= 0:
        return []
    batches: list[list[int]] = []
    for i in range(0, len(sorted_ids), batch_size):
        batches.append(sorted_ids[i : i + batch_size])
    return batches
