"""Postgres metadata read repository."""

from __future__ import annotations

from lakesource.table_config import TableConfig


class PostgresMetadataReadRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def fetch_atlas_area_chunk(self, chunk_start, chunk_end):
        from lakesource.postgres import area_quality as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_atlas_area_chunk(conn, chunk_start, chunk_end, table_config=self._tc)

    def fetch_atlas_area_by_ids(self, id_list):
        if not id_list:
            return {}
        return self.fetch_atlas_area_chunk(min(id_list), max(id_list) + 1)

    def fetch_seasonal_amplitude_chunk(self, chunk_start, chunk_end):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_seasonal_amplitude_chunk(conn, chunk_start, chunk_end, table_config=self._tc)

    def fetch_seasonal_amplitude_by_ids(self, id_list):
        by_range = self.fetch_seasonal_amplitude_chunk(min(id_list), max(id_list) + 1)
        return {hid: by_range.get(hid) for hid in id_list}

    def fetch_linear_trend_by_ids(self, id_list):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_linear_trend_by_ids(conn, id_list, table_config=self._tc)

    def fetch_max_hylak_id(self):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            result = _mod.fetch_max_lake_info_hylak_id(conn, table_config=self._tc)
        return result if result else 0

    def count_source_hylak_ids_in_range(self, chunk_start, chunk_end):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            return _mod.count_source_hylak_ids_in_range(conn, chunk_start, chunk_end, table_config=self._tc)

    def fetch_source_hylak_ids_in_range(self, chunk_start, chunk_end):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_source_hylak_ids_in_range(conn, chunk_start, chunk_end, table_config=self._tc)

    def fetch_frozen_year_months_chunk(self, chunk_start, chunk_end):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end, table_config=self._tc)

    def fetch_frozen_year_months_by_ids(self, id_list):
        from lakesource.postgres import lake_misc as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_frozen_year_months_by_ids(conn, id_list, table_config=self._tc)
