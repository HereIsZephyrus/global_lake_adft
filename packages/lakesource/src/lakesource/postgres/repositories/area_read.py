"""Postgres LakeArea read repository."""

from __future__ import annotations

from lakesource.table_config import TableConfig


class PostgresLakeAreaReadRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def fetch_lake_area_chunk(self, chunk_start, chunk_end):
        from lakesource.postgres import lake_area as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_lake_area_chunk(conn, chunk_start, chunk_end, table_config=self._tc)

    def fetch_lake_area_by_ids(self, id_list):
        from lakesource.postgres import lake_area as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_lake_area_by_ids(conn, id_list, table_config=self._tc)
