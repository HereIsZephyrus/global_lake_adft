"""Postgres comparison/interpolation/entropy write repositories."""

from __future__ import annotations

from lakesource.table_config import TableConfig


class PostgresComparisonWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_comparison_tables(self):
        from lakesource.postgres import comparison_schema as _mod
        with self._conn_factory() as conn:
            _mod.ensure_comparison_tables(conn, table_config=self._tc)

    def upsert_comparison_run_status(self, rows):
        from lakesource.postgres import comparison_schema as _mod
        with self._conn_factory() as conn:
            _mod.upsert_comparison_run_status(conn, rows, table_config=self._tc)

    def fetch_comparison_status_ids_in_range(self, chunk_start, chunk_end, *, workflow_version):
        from lakesource.postgres import comparison_schema as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_comparison_status_ids_in_range(conn, chunk_start, chunk_end, workflow_version=workflow_version, table_config=self._tc)


class PostgresInterpolationDetectWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_interpolation_detect_table(self):
        from lakesource.postgres import interpolation_detect_schema as _mod
        with self._conn_factory() as conn:
            _mod.ensure_interpolation_detect_table(conn, table_config=self._tc)

    def upsert_interpolation_detect(self, rows):
        from lakesource.postgres import interpolation_detect_schema as _mod
        with self._conn_factory() as conn:
            _mod.upsert_interpolation_detect(conn, rows, table_config=self._tc)


class PostgresEntropyWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_entropy_table(self):
        from lakesource.postgres import lake_entropy as _mod
        with self._conn_factory() as conn:
            _mod.ensure_entropy_table(conn, table_config=self._tc)

    def upsert_entropy(self, rows):
        from lakesource.postgres import lake_entropy as _mod
        with self._conn_factory() as conn:
            _mod.upsert_entropy(conn, rows, table_config=self._tc)

    def ensure_area_entropy_cv_table(self):
        from lakesource.postgres import lake_entropy as _mod
        with self._conn_factory() as conn:
            _mod.ensure_area_entropy_cv_table(conn, table_config=self._tc)

    def upsert_area_entropy_cv(self, rows):
        from lakesource.postgres import lake_entropy as _mod
        with self._conn_factory() as conn:
            _mod.upsert_area_entropy_cv(conn, rows, table_config=self._tc)
