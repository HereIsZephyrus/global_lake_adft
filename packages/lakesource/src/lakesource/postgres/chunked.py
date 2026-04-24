"""Chunked lake-ID processing with checkpoint/resume support.

Provides ``ChunkedLakeProcessor``, which splits the full hylak_id space into
fixed-size integer ranges and tracks progress via a configurable ``done_table``
row counts in SERIES_DB.  A chunk is considered done when every ``lake_info``
row in its ID range has a corresponding row in ``done_table``.  Results are
persisted immediately after each chunk, so an interrupted run can safely resume
from the next pending chunk on restart.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
from psycopg import sql as psql

from lakesource.postgres.client import DBClient
from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


class ChunkedLakeProcessor:
    """Process lake records in fixed-size hylak_id chunks with checkpoint/resume support.

    Checkpoint detection compares row counts in ``done_table`` against ``lake_info``
    for each chunk's ID range.  A chunk is skipped when its done-count equals
    its source-count.  Each chunk's results must be persisted by the caller's
    ``upsert_fn`` immediately after ``process_fn`` returns to keep the checkpoint
    state consistent.

    Example::

        processor = ChunkedLakeProcessor(series_db, chunk_size=10_000, done_table="lake_pfaf")
        processor.run(process_fn=my_process, upsert_fn=my_upsert)
    """

    def __init__(
        self,
        series_db: DBClient,
        chunk_size: int = 10_000,
        done_table: str = "lake_pfaf",
        table_config: TableConfig = _default_table_config,
    ) -> None:
        """Initialize the processor.

        Args:
            series_db: DBClient for SERIES_DB (source of lake_info and done_table).
            chunk_size: Number of consecutive hylak_id values per chunk.
            done_table: Table name used to detect already-completed chunks.
                        Must have a ``hylak_id`` column.
            table_config: TableConfig for resolving logical table names.
        """
        self._series_db = series_db
        self._chunk_size = chunk_size
        self._done_table = done_table
        self._table_config = table_config

    @property
    def chunk_size(self) -> int:
        """Number of hylak_id values per chunk."""
        return self._chunk_size

    def _fetch_max_hylak_id_sql(self) -> psql.Composed:
        return psql.SQL("SELECT MAX(hylak_id) FROM {}").format(
            psql.Identifier(self._table_config.series_table("lake_info"))
        )

    def _count_source_in_range_sql(self) -> psql.Composed:
        return psql.SQL(
            "SELECT COUNT(*) FROM {} WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s"
        ).format(psql.Identifier(self._table_config.series_table("lake_info")))

    def _max_hylak_id(self, conn: psycopg.Connection) -> int | None:
        """Return the maximum hylak_id in lake_info, or None if the table is empty.

        Args:
            conn: An open psycopg connection to SERIES_DB.

        Returns:
            Maximum hylak_id or None.
        """
        with conn.cursor() as cur:
            cur.execute(self._fetch_max_hylak_id_sql())
            row = cur.fetchone()
        max_id = int(row[0]) if row and row[0] is not None else None
        log.debug("Max hylak_id in lake_info: %s", max_id)
        return max_id

    def _is_chunk_done(
        self, conn: psycopg.Connection, chunk_start: int, chunk_end: int
    ) -> bool:
        """Return True if every lake_info row in [chunk_start, chunk_end) is in done_table.

        Args:
            conn: An open psycopg connection to SERIES_DB.
            chunk_start: Inclusive lower bound of the hylak_id range.
            chunk_end: Exclusive upper bound of the hylak_id range.

        Returns:
            True when done_count >= source_count (including empty source ranges).
        """
        count_done_sql = psql.SQL(
            "SELECT COUNT(*) FROM {} WHERE hylak_id >= %(chunk_start)s AND hylak_id < %(chunk_end)s"
        ).format(psql.Identifier(self._done_table))
        params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
        with conn.cursor() as cur:
            cur.execute(self._count_source_in_range_sql(), params)
            source_count: int = cur.fetchone()[0]
            if source_count == 0:
                return True
            cur.execute(count_done_sql, params)
            done_count: int = cur.fetchone()[0]
        return done_count >= source_count

    def iter_all_chunks(
        self, limit_id: int | None = None
    ) -> Iterator[tuple[int, int]]:
        """Yield (chunk_start, chunk_end) for every chunk covering the hylak_id range.

        The upper bound of the last chunk may exceed MAX(hylak_id); callers
        should treat it as an exclusive boundary.

        Args:
            limit_id: If given, chunks are bounded so that no ID >= limit_id is yielded.

        Yields:
            (chunk_start, chunk_end) pairs in ascending order.
        """
        with self._series_db.connection_context() as conn:
            max_id = self._max_hylak_id(conn)
        if max_id is None:
            return
        if limit_id is not None:
            max_id = min(max_id, limit_id - 1)
        for start in range(0, max_id + 1, self._chunk_size):
            chunk_end = start + self._chunk_size
            if limit_id is not None:
                chunk_end = min(chunk_end, limit_id)
            yield start, chunk_end

    def run(
        self,
        process_fn: Callable[[int, int], Any],
        upsert_fn: Callable[[Any], None],
        limit_id: int | None = None,
    ) -> None:
        """Run all pending chunks, skipping those already recorded in done_table.

        For each chunk that is not yet fully done:
          1. ``process_fn(chunk_start, chunk_end)`` is called to compute results
             for that hylak_id range.
          2. ``upsert_fn(result)`` is called to persist the result immediately.

        Completed chunks are detected on the next run by comparing row counts
        in ``done_table`` vs ``lake_info`` for each ID range, enabling safe
        resume without re-processing.

        Args:
            process_fn: Callable(chunk_start, chunk_end) -> result.
                        Receives the inclusive start and exclusive end of the
                        hylak_id range and returns any value accepted by upsert_fn.
            upsert_fn: Callable(result) -> None; must persist the result so
                       the chunk is recognised as done on the next run.
            limit_id: If given, only chunks with hylak_id < limit_id are processed.
        """
        all_chunks = list(self.iter_all_chunks(limit_id=limit_id))
        total = len(all_chunks)
        skipped = 0
        log.info("Starting chunked run: %d chunk(s), chunk_size=%d", total, self._chunk_size)

        for idx, (chunk_start, chunk_end) in enumerate(all_chunks, 1):
            with self._series_db.connection_context() as conn:
                already_done = self._is_chunk_done(conn, chunk_start, chunk_end)
            if already_done:
                skipped += 1
                log.debug(
                    "[%d/%d] chunk %d-%d: already done, skipping",
                    idx, total, chunk_start, chunk_end - 1,
                )
                continue

            log.info("[%d/%d] chunk %d-%d: processing...", idx, total, chunk_start, chunk_end - 1)
            result = process_fn(chunk_start, chunk_end)
            upsert_fn(result)

            log.info(
                "[%d/%d] chunk %d-%d: done (%d item(s))",
                idx, total, chunk_start, chunk_end - 1, len(result),
            )

        processed = total - skipped
        log.info("Done. %d chunk(s) processed, %d skipped.", processed, skipped)
