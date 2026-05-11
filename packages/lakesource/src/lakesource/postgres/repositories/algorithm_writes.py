"""Postgres algorithm result write repositories."""

from __future__ import annotations

from lakesource.table_config import TableConfig


class PostgresQuantileWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_quantile_tables(self):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            _mod.ensure_quantile_tables(conn, table_config=self._tc)

    def upsert_quantile_labels(self, rows):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            _mod.upsert_quantile_labels(conn, rows, table_config=self._tc)

    def upsert_quantile_extremes(self, rows):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            _mod.upsert_quantile_extremes(conn, rows, table_config=self._tc)

    def upsert_quantile_abrupt_transitions(self, rows):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            _mod.upsert_quantile_abrupt_transitions(conn, rows, table_config=self._tc)

    def upsert_quantile_run_status(self, rows):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            _mod.upsert_quantile_run_status(conn, rows, table_config=self._tc)

    def count_quantile_status_in_range(self, chunk_start, chunk_end, *, workflow_version):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            return _mod.count_quantile_status_in_range(conn, chunk_start, chunk_end, workflow_version=workflow_version, table_config=self._tc)

    def fetch_quantile_status_ids_in_range(self, chunk_start, chunk_end, *, workflow_version):
        from lakesource.postgres import lake_quantile as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_quantile_status_ids_in_range(conn, chunk_start, chunk_end, workflow_version=workflow_version, table_config=self._tc)


class PostgresPwmWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_pwm_extreme_tables(self):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.ensure_pwm_extreme_tables(conn, table_config=self._tc)

    def upsert_pwm_extreme_thresholds(self, rows):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.upsert_pwm_extreme_thresholds(conn, rows, table_config=self._tc)

    def upsert_pwm_extreme_labels(self, rows):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.upsert_pwm_extreme_labels(conn, rows, table_config=self._tc)

    def upsert_pwm_extreme_extremes(self, rows):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.upsert_pwm_extreme_extremes(conn, rows, table_config=self._tc)

    def upsert_pwm_extreme_abrupt_transitions(self, rows):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.upsert_pwm_extreme_abrupt_transitions(conn, rows, table_config=self._tc)

    def upsert_pwm_extreme_run_status(self, rows):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.upsert_pwm_extreme_run_status(conn, rows, table_config=self._tc)

    def upsert_pwm_hawkes_run_status(self, rows):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            _mod.upsert_pwm_hawkes_run_status(conn, rows, table_config=self._tc)

    def count_pwm_extreme_status_in_range(self, chunk_start, chunk_end, *, workflow_version):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            return _mod.count_pwm_extreme_status_in_range(conn, chunk_start, chunk_end, workflow_version=workflow_version, table_config=self._tc)

    def fetch_pwm_extreme_status_ids_in_range(self, chunk_start, chunk_end, *, workflow_version):
        from lakesource.postgres import lake_pwm as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_pwm_extreme_status_ids_in_range(conn, chunk_start, chunk_end, workflow_version=workflow_version, table_config=self._tc)


class PostgresEotWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_eot_results_table(self):
        from lakesource.postgres import lake_eot as _mod
        with self._conn_factory() as conn:
            _mod.ensure_eot_results_table(conn, table_config=self._tc)

    def upsert_eot_results(self, rows):
        from lakesource.postgres import lake_eot as _mod
        with self._conn_factory() as conn:
            _mod.upsert_eot_results(conn, rows, table_config=self._tc)

    def upsert_eot_extremes(self, rows):
        from lakesource.postgres import lake_eot as _mod
        with self._conn_factory() as conn:
            _mod.upsert_eot_extremes(conn, rows, table_config=self._tc)

    def upsert_eot_run_status(self, rows):
        from lakesource.postgres import lake_eot as _mod
        with self._conn_factory() as conn:
            _mod.upsert_eot_run_status(conn, rows, table_config=self._tc)


class PostgresHawkesWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_hawkes_results_table(self):
        from lakesource.postgres import lake_hawkes as _mod
        with self._conn_factory() as conn:
            _mod.ensure_hawkes_results_table(conn, table_config=self._tc)

    def upsert_hawkes_results(self, rows):
        from lakesource.postgres import lake_hawkes as _mod
        with self._conn_factory() as conn:
            _mod.upsert_hawkes_results(conn, rows, table_config=self._tc)

    def upsert_hawkes_lrt(self, rows):
        from lakesource.postgres import lake_hawkes as _mod
        with self._conn_factory() as conn:
            _mod.upsert_hawkes_lrt(conn, rows, table_config=self._tc)

    def upsert_hawkes_transition_monthly(self, rows):
        from lakesource.postgres import lake_hawkes as _mod
        with self._conn_factory() as conn:
            _mod.upsert_hawkes_transition_monthly(conn, rows, table_config=self._tc)
