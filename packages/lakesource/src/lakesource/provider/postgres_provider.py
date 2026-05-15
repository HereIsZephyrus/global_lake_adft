"""PostgresLakeProvider: backend reads and grid aggregations via psycopg.

Refactored to use PostgresBackend internally, eliminating if/elif table_name
dispatch.  The old ensure_table / upsert_rows / fetch_rows API is preserved
for backward compatibility but delegates to typed domain repositories.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
from psycopg import sql as psql

from lakesource.config import SourceConfig

from .base import LakeProvider

_SAFE_TABLE_NAME = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _table_ident(table_name: str) -> psql.Identifier:
    """Validate and wrap a table name for safe SQL composition."""
    if not _SAFE_TABLE_NAME.match(table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    return psql.Identifier(table_name)


def _ensure_queries_registered() -> None:
    from lakesource.provider.grid_query import list_grid_queries
    if not list_grid_queries():
        import lakesource.quantile.grid_queries  # noqa: F401  # pylint: disable=unused-import
        import lakesource.pwm.grid_queries  # noqa: F401
        import lakesource.eot.grid_queries  # noqa: F401
        import lakesource.comparison.grid_queries  # noqa: F401


# ------------------------------------------------------------------
# Domain method dispatch maps (env => (model, method))
# Used only by the legacy string-based API.
# ------------------------------------------------------------------

_ENSURE_DISPATCH = {
    # Algorithm-level keys (legacy)
    "area_quality": ("quality", "ensure_area_quality_table"),
    "area_anomalies": ("anomalies", "ensure_area_anomalies_table"),
    "area_shift_labels": ("shift_labels", "ensure_area_shift_labels_table"),
    "entropy": ("entropy", "ensure_entropy_table"),
    "interpolation_detect": ("interpolation", "ensure_interpolation_detect_table"),
    "quantile": ("quantile", "ensure_quantile_tables"),
    "pwm_extreme": ("pwm", "ensure_pwm_extreme_tables"),
    "hawkes": ("hawkes", "ensure_hawkes_results_table"),
    "pwm_hawkes": ("hawkes", "ensure_pwm_hawkes_tables"),
    "eot_hawkes": ("hawkes", "ensure_eot_hawkes_tables"),
    "eot": ("eot", "ensure_eot_results_table"),
    "comparison": ("comparison", "ensure_comparison_tables"),
    # Individual table-level keys (used by batch engine ensure_schema)
    "quantile_labels": ("quantile", "ensure_quantile_tables"),
    "quantile_extremes": ("quantile", "ensure_quantile_tables"),
    "quantile_abrupt_transitions": ("quantile", "ensure_quantile_tables"),
    "quantile_run_status": ("quantile", "ensure_quantile_tables"),
    "pwm_extreme_thresholds": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_extreme_labels": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_extreme_extremes": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_extreme_return_levels": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_extreme_abrupt_transitions": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_extreme_run_status": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_hawkes_run_status": ("pwm", "ensure_pwm_extreme_tables"),
    "pwm_hawkes_results": ("hawkes", "ensure_pwm_hawkes_tables"),
    "pwm_hawkes_lrt": ("hawkes", "ensure_pwm_hawkes_tables"),
    "pwm_hawkes_transition_monthly": ("hawkes", "ensure_pwm_hawkes_tables"),
    "pwm_hawkes_segments": ("pwm", "ensure_pwm_extreme_tables"),
    "eot_hawkes_results": ("hawkes", "ensure_eot_hawkes_tables"),
    "eot_hawkes_lrt": ("hawkes", "ensure_eot_hawkes_tables"),
    "eot_hawkes_transition_monthly": ("hawkes", "ensure_eot_hawkes_tables"),
    "eot_hawkes_run_status": ("hawkes", "ensure_eot_hawkes_tables"),
    "eot_results": ("eot", "ensure_eot_results_table"),
    "eot_extremes": ("eot", "ensure_eot_results_table"),
    "eot_return_levels": ("eot", "ensure_eot_results_table"),
    "eot_run_status": ("eot", "ensure_eot_results_table"),
    "comparison_run_status": ("comparison", "ensure_comparison_tables"),
}

_UPSERT_DISPATCH = {
    "area_quality": ("quality", "upsert_area_quality"),
    "area_anomalies": ("anomalies", "upsert_area_anomalies"),
    "area_shift_labels": ("shift_labels", "upsert_area_shift_labels"),
    "entropy": ("entropy", "upsert_entropy"),
    "interpolation_detect": ("interpolation", "upsert_interpolation_detect"),
    "quantile_labels": ("quantile", "upsert_quantile_labels"),
    "quantile_extremes": ("quantile", "upsert_quantile_extremes"),
    "quantile_abrupt_transitions": ("quantile", "upsert_quantile_abrupt_transitions"),
    "quantile_run_status": ("quantile", "upsert_quantile_run_status"),
    "pwm_extreme_thresholds": ("pwm", "upsert_pwm_extreme_thresholds"),
    "pwm_extreme_labels": ("pwm", "upsert_pwm_extreme_labels"),
    "pwm_extreme_extremes": ("pwm", "upsert_pwm_extreme_extremes"),
    "pwm_extreme_return_levels": ("pwm", "upsert_pwm_extreme_return_levels"),
    "pwm_extreme_abrupt_transitions": ("pwm", "upsert_pwm_extreme_abrupt_transitions"),
    "pwm_extreme_run_status": ("pwm", "upsert_pwm_extreme_run_status"),
    "pwm_hawkes_run_status": ("pwm", "upsert_pwm_hawkes_run_status"),
    "pwm_hawkes_results": ("hawkes", "upsert_pwm_hawkes_results"),
    "pwm_hawkes_lrt": ("hawkes", "upsert_pwm_hawkes_lrt"),
    "pwm_hawkes_transition_monthly": ("hawkes", "upsert_pwm_hawkes_transition_monthly"),
    "pwm_hawkes_segments": ("pwm", "upsert_pwm_hawkes_segments"),
    "eot_hawkes_results": ("hawkes", "upsert_eot_hawkes_results"),
    "eot_hawkes_lrt": ("hawkes", "upsert_eot_hawkes_lrt"),
    "eot_hawkes_transition_monthly": ("hawkes", "upsert_eot_hawkes_transition_monthly"),
    "eot_hawkes_run_status": ("hawkes", "upsert_eot_hawkes_run_status"),
    "eot_results": ("eot", "upsert_eot_results"),
    "eot_extremes": ("eot", "upsert_eot_extremes"),
    "eot_return_levels": ("eot", "upsert_eot_return_levels"),
    "eot_run_status": ("eot", "upsert_eot_run_status"),
    "comparison_run_status": ("comparison", "upsert_comparison_run_status"),
}


class PostgresLakeProvider(LakeProvider):
    """Postgres provider backed by typed domain repositories."""

    def __init__(self, config: SourceConfig | None = None) -> None:
        self._config = config or SourceConfig()
        self._tc = self._config.t
        self._backend = None  # lazily created

    @property
    def _be(self):
        """Lazily create PostgresBackend."""
        if self._backend is None:
            from lakesource.postgres.backend import PostgresBackend
            self._backend = PostgresBackend.from_config(self._config)
        return self._backend

    def _conn(self):
        from lakesource.postgres import series_db  # pylint: disable=no-name-in-module
        return series_db.connection_context()

    # ------------------------------------------------------------------
    # Core reads (delegate to repositories)
    # ------------------------------------------------------------------

    def fetch_lake_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, pd.DataFrame]:
        return self._be.area.fetch_lake_area_chunk(chunk_start, chunk_end)

    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
        return self._be.area.fetch_lake_area_by_ids(id_list)

    def fetch_frozen_year_months_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, set[int]]:
        return self._be.metadata.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

    def fetch_frozen_year_months_by_ids(
        self, id_list: list[int]
    ) -> dict[int, set[int]]:
        return self._be.metadata.fetch_frozen_year_months_by_ids(id_list)

    def fetch_atlas_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float]:
        return self._be.metadata.fetch_atlas_area_chunk(chunk_start, chunk_end)

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        return self._be.metadata.fetch_atlas_area_by_ids(id_list)

    def fetch_seasonal_amplitude_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float | None]:
        return self._be.metadata.fetch_seasonal_amplitude_chunk(chunk_start, chunk_end)

    def fetch_seasonal_amplitude_by_ids(self, id_list: list[int]) -> dict[int, float | None]:
        return self._be.metadata.fetch_seasonal_amplitude_by_ids(id_list)

    def fetch_max_hylak_id(self) -> int:
        return self._be.metadata.fetch_max_hylak_id()

    # ------------------------------------------------------------------
    # Table management (delegate to typed repos + dispatch for compat)
    # ------------------------------------------------------------------

    def ensure_table(self, table_name: str) -> None:
        info = _ENSURE_DISPATCH.get(table_name)
        if info is None:
            # Fallback: handle special cases not in typed dispatch
            if table_name == "lake_pfaf":
                from lakeanalysis.artificial.pfaf.store import ensure_lake_pfaf_table
                with self._conn() as conn:
                    ensure_lake_pfaf_table(conn)
                return
            if table_name == "af_nearest":
                from lakeanalysis.artificial.pfaf.store import ensure_af_nearest_table
                with self._conn() as conn:
                    ensure_af_nearest_table(conn)
                return
            raise ValueError(f"Unsupported table ensure: {table_name}")
        repo_name, method_name = info
        repo = getattr(self._be, repo_name)
        getattr(repo, method_name)()

        # Side-effect: area_quality also ensures area_anomalies
        if table_name == "area_quality":
            self._be.anomalies.ensure_area_anomalies_table()
        # Side-effect: comparison also ensures quantile and pwm tables
        if table_name == "comparison":
            self._be.quantile.ensure_quantile_tables()
            self._be.pwm.ensure_pwm_extreme_tables()

    def truncate_table(self, table_name: str) -> None:
        self._be.shift_labels.truncate_area_shift_labels() if table_name == "area_shift_labels" else exec_raw_truncate(table_name, self._conn)

    def upsert_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        info = _UPSERT_DISPATCH.get(table_name)
        if info is None:
            if table_name == "lake_pfaf":
                from lakeanalysis.artificial.pfaf.store import upsert_lake_pfaf
                with self._conn() as conn:
                    upsert_lake_pfaf(conn, {int(r["hylak_id"]): r.get("pfaf_id") for r in rows})
                return
            if table_name == "af_nearest":
                from lakeanalysis.artificial.pfaf.store import upsert_af_nearest
                with self._conn() as conn:
                    upsert_af_nearest(conn, rows)
                return
            raise ValueError(f"Unsupported table upsert: {table_name}")
        repo_name, method_name = info
        repo = getattr(self._be, repo_name)
        getattr(repo, method_name)(rows)

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict[str, Any]]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    psql.SQL("SELECT * FROM {} WHERE hylak_id >= %s AND hylak_id < %s").format(
                        _table_ident(table_name)
                    ),
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
                cur.execute(
                    psql.SQL("DELETE FROM {} WHERE hylak_id = ANY(%s)").format(
                        _table_ident(table_name)
                    ),
                    [hylak_ids],
                )
            conn.commit()

    # ------------------------------------------------------------------
    # Status & flag operations
    # ------------------------------------------------------------------

    def fetch_area_statuses(self) -> dict[int, tuple[str, int]]:
        return self._be.quality_read.fetch_area_statuses()

    def fetch_done_ids(
        self,
        table_name: str,
        chunk_start: int,
        chunk_end: int,
        *,
        status: str | None = None,
    ) -> set[int]:
        with self._conn() as conn:
            with conn.cursor() as cur:
                table = _table_ident(table_name)
                if status is None:
                    cur.execute(
                        psql.SQL(
                            "SELECT DISTINCT hylak_id FROM {} "
                            "WHERE hylak_id >= %s AND hylak_id < %s"
                        ).format(table),
                        (chunk_start, chunk_end),
                    )
                else:
                    cur.execute(
                        psql.SQL(
                            "SELECT DISTINCT hylak_id FROM {} "
                            "WHERE hylak_id >= %s AND hylak_id < %s AND status = %s"
                        ).format(table),
                        (chunk_start, chunk_end, status),
                    )
                return {int(row[0]) for row in cur.fetchall()}

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
        self._be.anomalies.update_area_anomaly_flags(updates)

    def fetch_impact_pairs(self) -> list[dict[str, int]]:
        return self._be.geometry.fetch_impact_pairs()

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

    def fetch_lake_geometry_wkt_by_ids(
        self,
        hylak_ids: list[int],
        *,
        simplify_tolerance_meters: float | None = None,
    ) -> pd.DataFrame:
        return self._be.geometry.fetch_lake_geometry_wkt_by_ids(
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


def exec_raw_truncate(table_name: str, conn_factory) -> None:
    with conn_factory() as conn:
        with conn.cursor() as cur:
            cur.execute(psql.SQL("TRUNCATE {}").format(_table_ident(table_name)))
        conn.commit()
