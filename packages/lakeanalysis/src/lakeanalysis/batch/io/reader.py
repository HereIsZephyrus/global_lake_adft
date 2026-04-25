"""Reader ABC and DBReader implementation."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd
import psycopg

from lakesource.postgres import (
    fetch_frozen_year_months_chunk,
    fetch_lake_area_chunk,
    fetch_max_area_quality_hylak_id,
    fetch_quantile_status_ids_in_range,
    fetch_pwm_extreme_status_ids_in_range,
    series_db,
)
from ..engine import Reader


class DBReader(Reader):
    def __init__(
        self,
        algorithm: str,
        *,
        workflow_version: str,
        conn_source=None,
    ) -> None:
        self._algorithm = algorithm
        self._workflow_version = workflow_version
        self._conn_source = conn_source or series_db

    def _conn(self):
        return self._conn_source.connection_context()

    def fetch_lake_map(self, chunk_start: int, chunk_end: int) -> dict[int, pd.DataFrame]:
        with self._conn() as conn:
            return fetch_lake_area_chunk(conn, chunk_start, chunk_end)

    def fetch_frozen_map(self, chunk_start: int, chunk_end: int) -> dict[int, set[int]]:
        with self._conn() as conn:
            return fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

    def fetch_done_ids(self, chunk_start: int, chunk_end: int) -> set[int]:
        with self._conn() as conn:
            if self._algorithm == "quantile":
                return fetch_quantile_status_ids_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._workflow_version,
                )
            if self._algorithm == "pwm_extreme":
                return fetch_pwm_extreme_status_ids_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._workflow_version,
                )
            if self._algorithm == "eot":
                return self._fetch_eot_done_ids(conn, chunk_start, chunk_end)
        return set()

    def max_hylak_id(self) -> int:
        with self._conn() as conn:
            result = fetch_max_area_quality_hylak_id(conn)
        return 0 if result is None else int(result)

    def ensure_schema(self) -> None:
        with self._conn() as conn:
            if self._algorithm == "quantile":
                from lakesource.postgres import ensure_quantile_tables
                ensure_quantile_tables(conn)
            elif self._algorithm == "pwm_extreme":
                from lakesource.postgres import ensure_pwm_extreme_tables
                ensure_pwm_extreme_tables(conn)
            elif self._algorithm == "eot":
                from lakesource.postgres import ensure_eot_results_table
                ensure_eot_results_table(conn)
                self._ensure_eot_run_status(conn)

    def _ensure_eot_run_status(self, conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS eot_run_status (
                    hylak_id BIGINT NOT NULL,
                    chunk_start BIGINT NOT NULL,
                    chunk_end BIGINT NOT NULL,
                    workflow_version VARCHAR(64) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (hylak_id, workflow_version)
                )
            """)
        conn.commit()

    def _fetch_eot_done_ids(
        self, conn: psycopg.Connection, chunk_start: int, chunk_end: int
    ) -> set[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT hylak_id
                FROM eot_run_status
                WHERE hylak_id >= %s AND hylak_id < %s
                  AND workflow_version = %s
                  AND status = 'done'
                """,
                (chunk_start, chunk_end, self._workflow_version),
            )
            return {int(row[0]) for row in cur.fetchall()}