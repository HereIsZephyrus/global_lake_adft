"""PostgresLakeProvider: full read/write implementation via psycopg."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import psycopg

from lakesource.config import SourceConfig
from lakesource.table_config import TableConfig

from .base import LakeProvider

log = logging.getLogger(__name__)


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

    def fetch_max_hylak_id(self) -> int:
        from lakesource.postgres import fetch_max_area_quality_hylak_id

        with self._conn() as conn:
            result = fetch_max_area_quality_hylak_id(conn)
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

    # ------------------------------------------------------------------
    # Algorithm-specific reads
    # ------------------------------------------------------------------

    def fetch_done_ids(
        self, algorithm: str, chunk_start: int, chunk_end: int
    ) -> set[int]:
        with self._conn() as conn:
            if algorithm == "quantile":
                from lakesource.postgres import fetch_quantile_status_ids_in_range

                return fetch_quantile_status_ids_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._config.workflow_version,
                )
            if algorithm == "pwm_extreme":
                from lakesource.postgres import fetch_pwm_extreme_status_ids_in_range

                return fetch_pwm_extreme_status_ids_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._config.workflow_version,
                )
            if algorithm == "eot":
                return self._fetch_eot_done_ids(conn, chunk_start, chunk_end)
            if algorithm == "comparison":
                from lakesource.postgres import fetch_comparison_status_ids_in_range

                return fetch_comparison_status_ids_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._config.workflow_version,
                )
        return set()

    def count_done_ids(
        self, algorithm: str, chunk_start: int, chunk_end: int
    ) -> int:
        with self._conn() as conn:
            if algorithm == "quantile":
                from lakesource.postgres import count_quantile_status_in_range

                return count_quantile_status_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._config.workflow_version,
                )
            if algorithm == "pwm_extreme":
                from lakesource.postgres import count_pwm_extreme_status_in_range

                return count_pwm_extreme_status_in_range(
                    conn, chunk_start, chunk_end,
                    workflow_version=self._config.workflow_version,
                )
        return 0

    def _fetch_eot_done_ids(
        self, conn: psycopg.Connection, chunk_start: int, chunk_end: int
    ) -> set[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT hylak_id
                FROM eot_run_status
                WHERE hylak_id >= %s AND hylak_id < %s
                  AND workflow_version = %s
                  AND status = 'done'
                """,
                (chunk_start, chunk_end, self._config.workflow_version),
            )
            return {int(row[0]) for row in cur.fetchall()}

    # ------------------------------------------------------------------
    # Aggregation reads (lakeviz global maps)
    # ------------------------------------------------------------------

    def fetch_extremes_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_extremes_grid_agg

        return fetch_extremes_grid_agg(self._config, resolution, refresh=refresh)

    def fetch_extremes_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_extremes_by_type_grid_agg

        return fetch_extremes_by_type_grid_agg(
            self._config, resolution, refresh=refresh
        )

    def fetch_transitions_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_transitions_grid_agg

        return fetch_transitions_grid_agg(self._config, resolution, refresh=refresh)

    def fetch_transitions_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.quantile.reader import fetch_transitions_by_type_grid_agg

        return fetch_transitions_by_type_grid_agg(
            self._config, resolution, refresh=refresh
        )

    def fetch_eot_convergence_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        from lakesource.eot.reader import fetch_eot_convergence_grid_agg

        return fetch_eot_convergence_grid_agg(
            self._config, tail, threshold_quantile, resolution, refresh=refresh
        )

    def fetch_eot_converged_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        from lakesource.eot.reader import fetch_eot_converged_grid_agg

        return fetch_eot_converged_grid_agg(
            self._config, tail, threshold_quantile, resolution, refresh=refresh
        )

    def fetch_pwm_convergence_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_convergence_grid_agg

        return fetch_pwm_convergence_grid_agg(
            self._config, resolution, refresh=refresh
        )

    def fetch_pwm_converged_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_converged_grid_agg

        return fetch_pwm_converged_grid_agg(
            self._config, resolution, refresh=refresh
        )

    def fetch_pwm_monthly_threshold_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_monthly_threshold_grid_agg

        return fetch_pwm_monthly_threshold_grid_agg(
            self._config, resolution, refresh=refresh
        )

    def fetch_pwm_exceedance_grid_agg(
        self, resolution: float = 0.5, *, p_high: float = 0.05, p_low: float = 0.05,
        refresh: bool = False,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_exceedance_grid_agg

        return fetch_pwm_exceedance_grid_agg(
            self._config, resolution, p_high=p_high, p_low=p_low, refresh=refresh
        )

    def fetch_pwm_monthly_exceedance_grid_agg(
        self, resolution: float = 0.5, *, p_high: float = 0.05, p_low: float = 0.05,
        refresh: bool = False,
    ) -> pd.DataFrame:
        from lakesource.pwm_extreme.reader import fetch_pwm_monthly_exceedance_grid_agg

        return fetch_pwm_monthly_exceedance_grid_agg(
            self._config, resolution, p_high=p_high, p_low=p_low, refresh=refresh
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        if not any(rows_by_table.values()):
            return
        with self._conn() as conn:
            try:
                for table_name, rows in rows_by_table.items():
                    if not rows:
                        continue
                    fn = self._get_upsert_fn(table_name)
                    if fn is None:
                        log.warning("No upsert function for table %s", table_name)
                        continue
                    fn(conn, rows, commit=False)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @staticmethod
    def _get_upsert_fn(table_name: str):
        _FNS = {
            "quantile_labels": "upsert_quantile_labels",
            "quantile_extremes": "upsert_quantile_extremes",
            "quantile_abrupt_transitions": "upsert_quantile_abrupt_transitions",
            "quantile_run_status": "upsert_quantile_run_status",
            "pwm_extreme_thresholds": "upsert_pwm_extreme_thresholds",
            "pwm_extreme_run_status": "upsert_pwm_extreme_run_status",
            "eot_results": "upsert_eot_results",
            "eot_extremes": "upsert_eot_extremes",
            "eot_run_status": "upsert_eot_run_status",
            "comparison_run_status": "upsert_comparison_run_status",
        }
        fn_name = _FNS.get(table_name)
        if fn_name is None:
            return None
        from lakesource.postgres import lake

        return getattr(lake, fn_name)

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def ensure_schema(self, algorithm: str) -> None:
        with self._conn() as conn:
            if algorithm == "quantile":
                from lakesource.postgres import ensure_quantile_tables

                ensure_quantile_tables(conn)
            elif algorithm == "pwm_extreme":
                from lakesource.postgres import ensure_pwm_extreme_tables

                ensure_pwm_extreme_tables(conn)
            elif algorithm == "eot":
                from lakesource.postgres import ensure_eot_results_table

                ensure_eot_results_table(conn)
                self._ensure_eot_run_status(conn)
            elif algorithm == "comparison":
                from lakesource.comparison import ensure_comparison_tables

                ensure_comparison_tables(conn)
                from lakesource.postgres import ensure_quantile_tables, ensure_pwm_extreme_tables

                ensure_quantile_tables(conn)
                ensure_pwm_extreme_tables(conn)

    @staticmethod
    def _ensure_eot_run_status(conn: psycopg.Connection) -> None:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS eot_run_status (
                    hylak_id BIGINT NOT NULL,
                    chunk_start BIGINT NOT NULL,
                    chunk_end BIGINT NOT NULL,
                    workflow_version VARCHAR(64) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    error_message TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (hylak_id, workflow_version)
                )
            """)
        conn.commit()

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    def backend_name(self) -> str:
        return "postgres"

    @property
    def cache_dir(self) -> Path | None:
        return None
