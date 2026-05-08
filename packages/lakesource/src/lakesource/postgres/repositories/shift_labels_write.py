"""Postgres shift_labels repository."""

from __future__ import annotations

from typing import Any
from lakesource.table_config import TableConfig


class PostgresShiftLabelsRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_area_shift_labels_table(self):
        from lakesource.postgres import area_quality as _mod
        with self._conn_factory() as conn:
            _mod.ensure_area_shift_labels_table(conn, table_config=self._tc)

    def truncate_area_shift_labels(self):
        from lakesource.postgres import area_quality as _mod
        with self._conn_factory() as conn:
            _mod.truncate_area_shift_labels(conn, table_config=self._tc)

    def upsert_area_shift_labels(self, rows: list[dict[str, Any]]):
        from lakesource.postgres import area_quality as _mod
        with self._conn_factory() as conn:
            _mod.upsert_area_shift_labels(conn, rows, table_config=self._tc)

    def fetch_shift_labels_in_range(self, chunk_start, chunk_end):
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                from psycopg import sql
                t = sql.Identifier(self._tc.series_table("area_shift_labels"))
                cur.execute(
                    sql.SQL("SELECT * FROM {table} WHERE hylak_id >= %(s)s::bigint AND hylak_id < %(e)s::bigint").format(table=t),
                    {"s": chunk_start, "e": chunk_end},
                )
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, r, strict=False)) for r in cur.fetchall()]
