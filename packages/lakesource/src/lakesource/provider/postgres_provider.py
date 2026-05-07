"""PostgresLakeProvider: backend reads and grid aggregations via psycopg."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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

    def fetch_done_ids(self, table_name: str, chunk_start: int, chunk_end: int) -> set[int]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT DISTINCT hylak_id FROM {table_name} WHERE hylak_id >= %s AND hylak_id < %s",
                    (chunk_start, chunk_end),
                )
                return {int(row[0]) for row in cur.fetchall()}

    def fetch_seasonal_amplitude_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float | None]:
        from lakesource.postgres import fetch_seasonal_amplitude_chunk

        with self._conn() as conn:
            return fetch_seasonal_amplitude_chunk(conn, chunk_start, chunk_end)

    def fetch_seasonal_amplitude_by_ids(self, id_list: list[int]) -> dict[int, float | None]:
        if not id_list:
            return {}
        by_range = self.fetch_seasonal_amplitude_chunk(min(id_list), max(id_list) + 1)
        return {hid: by_range.get(hid) for hid in id_list}

    def ensure_table(self, table_name: str) -> None:
        with self._conn() as conn:
            if table_name == "area_quality":
                from lakesource.postgres import ensure_area_anomalies_table, ensure_area_quality_table

                ensure_area_quality_table(conn)
                ensure_area_anomalies_table(conn)
            elif table_name == "area_anomalies":
                from lakesource.postgres import ensure_area_anomalies_table

                ensure_area_anomalies_table(conn)
            elif table_name == "entropy":
                from lakesource.postgres import ensure_entropy_table

                ensure_entropy_table(conn)
            elif table_name == "interpolation_detect":
                from lakesource.postgres import ensure_interpolation_detect_table

                ensure_interpolation_detect_table(conn)
            elif table_name == "lake_pfaf":
                from lakeanalysis.artificial.pfaf.store import ensure_lake_pfaf_table

                ensure_lake_pfaf_table(conn)
            elif table_name == "af_nearest":
                from lakeanalysis.artificial.pfaf.store import ensure_af_nearest_table

                ensure_af_nearest_table(conn)
            elif table_name == "quantile":
                from lakesource.postgres import ensure_quantile_tables

                ensure_quantile_tables(conn)
            elif table_name == "pwm_extreme":
                from lakesource.postgres import ensure_pwm_extreme_tables

                ensure_pwm_extreme_tables(conn)
            elif table_name == "eot":
                from lakesource.postgres import ensure_eot_results_table

                ensure_eot_results_table(conn)
            elif table_name == "comparison":
                from lakesource.postgres import ensure_comparison_tables, ensure_pwm_extreme_tables, ensure_quantile_tables

                ensure_comparison_tables(conn)
                ensure_quantile_tables(conn)
                ensure_pwm_extreme_tables(conn)
            else:
                raise ValueError(f"Unsupported table ensure: {table_name}")

    def truncate_table(self, table_name: str) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE {table_name}")
            conn.commit()

    def upsert_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self._conn() as conn:
            if table_name == "area_quality":
                from lakesource.postgres import upsert_area_quality

                upsert_area_quality(conn, rows)
            elif table_name == "area_anomalies":
                from lakesource.postgres import upsert_area_anomalies

                upsert_area_anomalies(conn, rows)
            elif table_name == "entropy":
                from lakesource.postgres import upsert_entropy

                upsert_entropy(conn, rows)
            elif table_name == "interpolation_detect":
                from lakesource.postgres import upsert_interpolation_detect

                upsert_interpolation_detect(conn, rows)
            elif table_name == "lake_pfaf":
                from lakeanalysis.artificial.pfaf.store import upsert_lake_pfaf

                upsert_lake_pfaf(conn, {int(row["hylak_id"]): row.get("pfaf_id") for row in rows})
            elif table_name == "af_nearest":
                from lakeanalysis.artificial.pfaf.store import upsert_af_nearest

                upsert_af_nearest(conn, rows)
            elif table_name == "quantile_labels":
                from lakesource.postgres import upsert_quantile_labels

                upsert_quantile_labels(conn, rows)
            elif table_name == "quantile_extremes":
                from lakesource.postgres import upsert_quantile_extremes

                upsert_quantile_extremes(conn, rows)
            elif table_name == "quantile_abrupt_transitions":
                from lakesource.postgres import upsert_quantile_abrupt_transitions

                upsert_quantile_abrupt_transitions(conn, rows)
            elif table_name == "quantile_run_status":
                from lakesource.postgres import upsert_quantile_run_status

                upsert_quantile_run_status(conn, rows)
            elif table_name == "pwm_extreme_thresholds":
                from lakesource.postgres import upsert_pwm_extreme_thresholds

                upsert_pwm_extreme_thresholds(conn, rows)
            elif table_name == "pwm_extreme_run_status":
                from lakesource.postgres import upsert_pwm_extreme_run_status

                upsert_pwm_extreme_run_status(conn, rows)
            elif table_name == "eot_results":
                from lakesource.postgres import upsert_eot_results

                upsert_eot_results(conn, rows)
            elif table_name == "eot_extremes":
                from lakesource.postgres import upsert_eot_extremes

                upsert_eot_extremes(conn, rows)
            elif table_name == "eot_run_status":
                from lakesource.postgres import upsert_eot_run_status

                upsert_eot_run_status(conn, rows)
            elif table_name == "comparison_run_status":
                from lakesource.postgres import upsert_comparison_run_status

                upsert_comparison_run_status(conn, rows)
            else:
                raise ValueError(f"Unsupported table upsert: {table_name}")

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict[str, Any]]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {table_name} WHERE hylak_id >= %s AND hylak_id < %s",
                    (chunk_start, chunk_end),
                )
                rows = cur.fetchall()
                columns = [desc.name for desc in cur.description]
        return [dict(zip(columns, row, strict=False)) for row in rows]

    def delete_ids(self, table_name: str, hylak_ids: list[int]) -> None:
        if not hylak_ids:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {table_name} WHERE hylak_id = ANY(%s)", [hylak_ids])
            conn.commit()

    def fetch_area_statuses(self) -> dict[int, tuple[str, int]]:
        result: dict[int, tuple[str, int]] = {}
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT hylak_id FROM area_quality")
                for (hid,) in cur.fetchall():
                    result[int(hid)] = ("quality", 0)
                cur.execute("SELECT hylak_id, anomaly_flags FROM area_anomalies")
                for hid, flags in cur.fetchall():
                    result[int(hid)] = ("anomalies", int(flags))
        return result

    def fetch_zero_quantile_flags(self) -> dict[int, int]:
        result: dict[int, int] = {}
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT hylak_id, anomaly_flags FROM area_anomalies WHERE anomaly_flags & %s > 0",
                    [1],
                )
                for hid, flags in cur.fetchall():
                    result[int(hid)] = int(flags)
        return result

    def clear_zero_quantile_flag(self, hylak_ids: list[int]) -> int:
        if not hylak_ids:
            return 0
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE area_anomalies SET anomaly_flags = anomaly_flags & ~%s WHERE hylak_id = ANY(%s)",
                    [1, hylak_ids],
                )
                updated = int(cur.rowcount or 0)
            conn.commit()
        return updated

    def find_nonzero_quantile_lakes(self, hylak_ids: list[int], quantile: float) -> set[int]:
        if not hylak_ids:
            return set()
        placeholders = ",".join(["%s"] * len(hylak_ids))
        result: set[int] = set()
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT la.hylak_id
                    FROM lake_area la
                    WHERE la.hylak_id IN ({placeholders})
                      AND NOT EXISTS (
                        SELECT 1 FROM anomaly a
                        WHERE a.hylak_id = la.hylak_id
                          AND a.anomaly_type = 'frozen'
                          AND a.year_month = la.year_month
                      )
                    GROUP BY la.hylak_id
                    HAVING PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY la.water_area) > 0
                    """,
                    hylak_ids + [quantile],
                )
                for (hid,) in cur.fetchall():
                    result.add(int(hid))
        return result

    def update_area_anomaly_flags(self, updates: list[tuple[int, int]]) -> None:
        if not updates:
            return
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE area_anomalies SET anomaly_flags = %s WHERE hylak_id = %s",
                    [(flags, hid) for hid, flags in updates],
                )
            conn.commit()

    def fetch_impact_pairs(self) -> list[dict[str, int]]:
        from lakesource.postgres import fetch_impact_pairs

        with self._conn() as conn:
            return fetch_impact_pairs(conn)

    def fetch_lake_centroids_chunk(self, chunk_start: int, chunk_end: int) -> list[tuple[int, str]]:
        from lakeanalysis.artificial.pfaf.lookup import fetch_lake_centroids_chunk

        with self._conn() as conn:
            return fetch_lake_centroids_chunk(conn, chunk_start, chunk_end)

    def lookup_pfaf_chunk(self, centroids: list[tuple[int, str]]) -> dict[int, int | None]:
        from lakeanalysis.artificial.pfaf.lookup import lookup_pfaf_chunk
        from lakesource.postgres import atlas_db

        with atlas_db.connection_context() as conn:
            return lookup_pfaf_chunk(conn, centroids)

    def fetch_type1_lake_records(self) -> list[dict[str, int | float | None]]:
        from lakeanalysis.artificial.pfaf.nearest import _FETCH_TYPE1_SQL

        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(_FETCH_TYPE1_SQL)
                rows = cur.fetchall()
        return [
            {
                "hylak_id": int(row[0]),
                "pfaf_id": int(row[1]),
                "lat": float(row[2]),
                "lon": float(row[3]),
                "lake_area": float(row[4]) if row[4] is not None else None,
            }
            for row in rows
        ]

    def fetch_non_type1_lake_records(
        self, limit_id: int | None = None
    ) -> list[dict[str, int | float | None]]:
        from lakeanalysis.artificial.pfaf.nearest import (
            _FETCH_NON_TYPE1_LIMITED_SQL,
            _FETCH_NON_TYPE1_SQL,
        )

        query = _FETCH_NON_TYPE1_SQL if limit_id is None else _FETCH_NON_TYPE1_LIMITED_SQL
        params = None if limit_id is None else {"limit_id": limit_id}
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
        return [
            {
                "hylak_id": int(row[0]),
                "lake_type": int(row[1]),
                "pfaf_id": int(row[2]),
                "lat": float(row[3]),
                "lon": float(row[4]),
                "lake_area": float(row[5]) if row[5] is not None else None,
            }
            for row in rows
        ]

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
