"""Postgres quality write repository."""

from __future__ import annotations

from typing import Any
from lakesource.table_config import TableConfig


class PostgresQualityWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_area_quality_table(self):
        from lakesource.postgres import area_quality_schema as _mod
        with self._conn_factory() as conn:
            _mod.ensure_area_quality_table(conn, table_config=self._tc)

    def upsert_area_quality(self, rows: list[dict[str, Any]]):
        from lakesource.postgres import area_quality_schema as _mod
        with self._conn_factory() as conn:
            _mod.upsert_area_quality(conn, rows, table_config=self._tc)

    def fetch_area_quality_hylak_ids_in_range(self, chunk_start, chunk_end):
        from lakesource.postgres import area_quality_schema as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_area_quality_hylak_ids_in_range(conn, chunk_start, chunk_end, table_config=self._tc)

    def count_area_quality_hylak_ids_in_range(self, chunk_start, chunk_end):
        from lakesource.postgres import area_quality_schema as _mod
        with self._conn_factory() as conn:
            return _mod.count_area_quality_hylak_ids_in_range(conn, chunk_start, chunk_end, table_config=self._tc)

    def delete_area_quality_by_ids(self, hylak_ids):
        from lakesource.postgres import area_quality_schema as _mod
        params = {"id_list": hylak_ids}
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute(_mod._delete_area_quality_by_ids_sql(self._tc), params)
            conn.commit()
