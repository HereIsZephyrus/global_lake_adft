"""Postgres anomalies write repository."""

from __future__ import annotations

from typing import Any
from lakesource.table_config import TableConfig


class PostgresAnomaliesWriteRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def ensure_area_anomalies_table(self):
        from lakesource.postgres import area_anomalies_schema as _mod
        with self._conn_factory() as conn:
            _mod.ensure_area_anomalies_table(conn, table_config=self._tc)

    def upsert_area_anomalies(self, rows: list[dict[str, Any]]):
        from lakesource.postgres import area_anomalies_schema as _mod
        with self._conn_factory() as conn:
            _mod.upsert_area_anomalies(conn, rows, table_config=self._tc)

    def move_area_quality_to_anomalies(self, hylak_ids):
        from lakesource.postgres import area_anomalies_schema as _mod
        with self._conn_factory() as conn:
            return _mod.move_area_quality_to_anomalies(conn, hylak_ids, table_config=self._tc)

    def update_area_anomaly_flags(self, updates: list[tuple[int, int]]):
        if not updates:
            return
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                from psycopg import sql
                t = sql.Identifier(self._tc.series_table("area_anomalies"))
                cur.execute(
                    sql.SQL("UPDATE {table} SET anomaly_flags = %(flags)s WHERE hylak_id = %(hid)s").format(table=t),
                    [{"hid": hid, "flags": flags} for hid, flags in updates],
                )
            conn.commit()

    def delete_area_anomalies_by_ids(self, hylak_ids):
        if not hylak_ids:
            return
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM area_anomalies WHERE hylak_id = ANY(%s)", [hylak_ids])
            conn.commit()
