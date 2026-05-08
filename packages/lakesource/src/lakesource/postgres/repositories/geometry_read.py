"""Postgres geometry read repository."""

from __future__ import annotations

import pandas as pd
from lakesource.table_config import TableConfig


class PostgresGeometryReadRepository:
    def __init__(self, connection_factory, *, table_config=None):
        self._conn_factory = connection_factory
        self._tc = table_config or TableConfig.default()

    def fetch_lake_geometry_wkt_by_ids(self, hylak_ids, *, simplify_tolerance_meters=None):
        from lakesource.postgres import lake as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_lake_geometry_wkt_by_ids(
                conn, hylak_ids,
                simplify_tolerance_meters=simplify_tolerance_meters,
                table_config=self._tc,
            )

    def fetch_lake_centroids_chunk(self, chunk_start, chunk_end):
        with self._conn_factory() as conn:
            with conn.cursor() as cur:
                from psycopg import sql
                table = self._tc.atlas_table("lake_geometry")
                parts = [p.strip() for p in table.split(".")]
                ident = sql.Identifier(parts[0], parts[1]) if len(parts) == 2 else sql.Identifier(parts[0])
                cur.execute(
                    sql.SQL("SELECT hylak_id, ST_AsText(ST_Centroid(ST_Transform(geom, 4326))) FROM {table} WHERE hylak_id >= %(s)s AND hylak_id < %(e)s").format(table=ident),
                    {"s": chunk_start, "e": chunk_end},
                )
                return [(int(r[0]), str(r[1])) for r in cur.fetchall()]

    def fetch_impact_pairs(self):
        from lakesource.postgres import lake_area as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_impact_pairs(conn, table_config=self._tc)

    def fetch_af_nearest_high_topo(self):
        from lakesource.postgres import lake_area as _mod
        with self._conn_factory() as conn:
            return _mod.fetch_af_nearest_high_topo(conn, table_config=self._tc)
