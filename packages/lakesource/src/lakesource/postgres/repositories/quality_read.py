"""Postgres quality read repository."""

from __future__ import annotations

from lakesource.table_config import TableConfig


class PostgresQualityReadRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def fetch_area_statuses(self) -> dict[int, tuple[str, int]]:
        result: dict[int, tuple[str, int]] = {}
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                qt = self._tc.series_table("area_quality")
                cur.execute(f"SELECT hylak_id FROM {qt} ORDER BY hylak_id")
                for row in cur.fetchall():
                    result[int(row[0])] = ("quality", 0)
                at = self._tc.series_table("area_anomalies")
                cur.execute(f"SELECT hylak_id, anomaly_flags FROM {at} ORDER BY hylak_id")
                for row in cur.fetchall():
                    result[int(row[0])] = ("anomalies", int(row[1]))
        return result

    def fetch_anomaly_hylak_ids(self) -> set[int]:
        from lakesource.postgres import area_cross_queries as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_anomaly_hylak_ids(conn, table_config=self._tc)
