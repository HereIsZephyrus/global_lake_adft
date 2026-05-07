"""PostgresLakeProvider: backend reads and grid aggregations via psycopg."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig

from .base import LakeProvider


def _ensure_queries_registered() -> None:
    from lakesource.provider.grid_query import list_grid_queries
    if not list_grid_queries():
        import lakesource.quantile.grid_queries  # noqa: F401
        import lakesource.pwm_extreme.grid_queries  # noqa: F401
        import lakesource.eot.grid_queries  # noqa: F401
        import lakesource.comparison.grid_queries  # noqa: F401


class PostgresLakeProvider(LakeProvider):
    def __init__(self, config: SourceConfig | None = None) -> None:
        self._config = config or SourceConfig()
        self._tc = self._config.t

    def _conn(self):
        from lakesource.postgres import series_db

        return series_db.connection_context()

    # ------------------------------------------------------------------
    # Core reads
    # ------------------------------------------------------------------

    def fetch_lake_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, pd.DataFrame]:
        from lakesource.postgres import fetch_lake_area_chunk

        with self._conn() as conn:
            return fetch_lake_area_chunk(conn, chunk_start, chunk_end)

    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
        from lakesource.postgres import fetch_lake_area_by_ids

        with self._conn() as conn:
            return fetch_lake_area_by_ids(conn, id_list)

    def fetch_frozen_year_months_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, set[int]]:
        from lakesource.postgres import fetch_frozen_year_months_chunk

        with self._conn() as conn:
            return fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

    def fetch_frozen_year_months_by_ids(
        self, id_list: list[int]
    ) -> dict[int, set[int]]:
        from lakesource.postgres import fetch_frozen_year_months_by_ids

        with self._conn() as conn:
            return fetch_frozen_year_months_by_ids(conn, id_list)

    def fetch_atlas_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float]:
        from lakesource.postgres import fetch_atlas_area_chunk

        with self._conn() as conn:
            return fetch_atlas_area_chunk(conn, chunk_start, chunk_end)

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        if not id_list:
            return {}
        with self._conn() as conn:
            from lakesource.postgres import fetch_atlas_area_chunk

            return fetch_atlas_area_chunk(conn, min(id_list), max(id_list) + 1)

    def fetch_max_hylak_id(self) -> int:
        from lakesource.postgres import fetch_max_lake_info_hylak_id

        with self._conn() as conn:
            result = fetch_max_lake_info_hylak_id(conn)
        return 0 if result is None else int(result)

    def fetch_lake_geometry_wkt_by_ids(
        self,
        hylak_ids: list[int],
        *,
        simplify_tolerance_meters: float | None = None,
    ) -> pd.DataFrame:
        from lakesource.postgres import fetch_lake_geometry_wkt_by_ids

        with self._conn() as conn:
            return fetch_lake_geometry_wkt_by_ids(
                conn,
                hylak_ids,
                simplify_tolerance_meters=simplify_tolerance_meters,
            )

    def fetch_grid_agg(
        self,
        query_name: str,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        from lakesource.provider.grid_query import get_grid_query
        _ensure_queries_registered()
        query = get_grid_query(query_name)
        return query.fetch_postgres(self._config, resolution, refresh=refresh, **kwargs)

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    def backend_name(self) -> str:
        return "postgres"

    @property
    def cache_dir(self) -> Path | None:
        return None
