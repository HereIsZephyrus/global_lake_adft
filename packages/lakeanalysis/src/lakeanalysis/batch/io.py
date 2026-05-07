"""Batch IO abstractions and backend-specific implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd

from lakesource.comparison import ensure_comparison_tables
from lakesource.config import Backend, SourceConfig
from lakesource.parquet.client import DuckDBClient

log = logging.getLogger(__name__)

_APPEND_ONLY_TABLES = {
    "quantile_labels",
    "quantile_extremes",
    "quantile_abrupt_transitions",
    "quantile_run_status",
    "pwm_extreme_thresholds",
    "pwm_extreme_run_status",
    "eot_results",
    "eot_extremes",
    "eot_run_status",
    "comparison_run_status",
}


class BatchReader(ABC):
    @abstractmethod
    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int): ...

    @abstractmethod
    def fetch_lake_area_by_ids(self, id_list: list[int]): ...

    @abstractmethod
    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int): ...

    @abstractmethod
    def fetch_frozen_year_months_by_ids(self, id_list: list[int]): ...

    @abstractmethod
    def fetch_max_hylak_id(self) -> int: ...

    @abstractmethod
    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]: ...


class BatchWriter(ABC):
    @abstractmethod
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None: ...

    @abstractmethod
    def ensure_schema(self, algorithm: str) -> None: ...


class _PostgresBatchBase:
    def __init__(self, config: SourceConfig | None = None) -> None:
        self._config = config or SourceConfig()

    def _conn(self):
        from lakesource.postgres import series_db

        return series_db.connection_context()


class PostgresBatchReader(_PostgresBatchBase, BatchReader):
    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        from lakesource.postgres import fetch_lake_area_chunk

        with self._conn() as conn:
            return fetch_lake_area_chunk(conn, chunk_start, chunk_end)

    def fetch_lake_area_by_ids(self, id_list: list[int]):
        from lakesource.postgres import fetch_lake_area_by_ids

        with self._conn() as conn:
            return fetch_lake_area_by_ids(conn, id_list)

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        from lakesource.postgres import fetch_frozen_year_months_chunk

        with self._conn() as conn:
            return fetch_frozen_year_months_chunk(conn, chunk_start, chunk_end)

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        from lakesource.postgres import fetch_frozen_year_months_by_ids

        with self._conn() as conn:
            return fetch_frozen_year_months_by_ids(conn, id_list)

    def fetch_max_hylak_id(self) -> int:
        from lakesource.postgres import fetch_max_lake_info_hylak_id

        with self._conn() as conn:
            result = fetch_max_lake_info_hylak_id(conn)
        return 0 if result is None else int(result)

    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
        with self._conn() as conn:
            if algorithm == "quantile":
                from lakesource.postgres import fetch_quantile_status_ids_in_range

                return fetch_quantile_status_ids_in_range(
                    conn,
                    chunk_start,
                    chunk_end,
                    workflow_version=self._config.workflow_version,
                )
            if algorithm == "pwm_extreme":
                from lakesource.postgres import fetch_pwm_extreme_status_ids_in_range

                return fetch_pwm_extreme_status_ids_in_range(
                    conn,
                    chunk_start,
                    chunk_end,
                    workflow_version=self._config.workflow_version,
                )
            if algorithm == "comparison":
                from lakesource.postgres import fetch_comparison_status_ids_in_range

                return fetch_comparison_status_ids_in_range(
                    conn,
                    chunk_start,
                    chunk_end,
                    workflow_version=self._config.workflow_version,
                )
            if algorithm == "eot":
                return self._fetch_eot_done_ids(conn, chunk_start, chunk_end)
        return set()

    def _fetch_eot_done_ids(self, conn, chunk_start: int, chunk_end: int) -> set[int]:
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


class PostgresBatchWriter(_PostgresBatchBase, BatchWriter):
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
                ensure_comparison_tables(conn)
                from lakesource.postgres import ensure_quantile_tables, ensure_pwm_extreme_tables

                ensure_quantile_tables(conn)
                ensure_pwm_extreme_tables(conn)

    @staticmethod
    def _get_upsert_fn(table_name: str):
        mapping = {
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
        fn_name = mapping.get(table_name)
        if fn_name is None:
            return None
        from lakesource.postgres import lake

        return getattr(lake, fn_name)

    @staticmethod
    def _ensure_eot_run_status(conn) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
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
                """
            )
        conn.commit()


class _ParquetBatchBase:
    def __init__(self, config: SourceConfig) -> None:
        if config.data_dir is None:
            raise ValueError("data_dir is required for parquet batch IO")
        self._config = config
        self._data_dir = Path(config.data_dir)
        self._client = DuckDBClient(data_dir=self._data_dir, table_config=config.t)

    @staticmethod
    def _split_by_hylak_id(df: pd.DataFrame) -> dict[int, pd.DataFrame]:
        if df.empty:
            return {}
        result: dict[int, pd.DataFrame] = {}
        for hid, group in df.groupby("hylak_id"):
            result[int(hid)] = group.drop(columns=["hylak_id"]).reset_index(drop=True)
        return result

    @staticmethod
    def _frozen_from_anomaly_df(df: pd.DataFrame) -> dict[int, set[int]]:
        result: dict[int, set[int]] = {}
        if df.empty:
            return result
        if "year_month" in df.columns:
            df = df.copy()
            df["year"] = df["year_month"].dt.year.astype(int)
            df["month"] = df["year_month"].dt.month.astype(int)
        for hid, group in df.groupby("hylak_id"):
            frozen = set()
            for _, row in group.iterrows():
                if "year" in row and "month" in row:
                    frozen.add(int(row["year"]) * 100 + int(row["month"]))
            result[int(hid)] = frozen
        return result


class ParquetBatchReader(_ParquetBatchBase, BatchReader):
    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        df = self._client.query_df(
            """
            SELECT la.hylak_id,
                   YEAR(la.year_month)  AS year,
                   MONTH(la.year_month) AS month,
                   la.water_area
            FROM lake_area la
            WHERE la.hylak_id >= ? AND la.hylak_id < ?
            ORDER BY la.hylak_id, la.year_month
            """,
            parameters=[chunk_start, chunk_end],
        )
        if df.empty:
            return {}
        df["year"] = df["year"].astype(int)
        df["month"] = df["month"].astype(int)
        df["water_area"] = df["water_area"].astype(float)
        return self._split_by_hylak_id(df)

    def fetch_lake_area_by_ids(self, id_list: list[int]):
        if not id_list:
            return {}
        placeholders = ",".join("?" for _ in id_list)
        df = self._client.query_df(
            f"""
            SELECT la.hylak_id,
                   YEAR(la.year_month)  AS year,
                   MONTH(la.year_month) AS month,
                   la.water_area
            FROM lake_area la
            WHERE la.hylak_id IN ({placeholders})
            ORDER BY la.hylak_id, la.year_month
            """,
            parameters=id_list,
        )
        if df.empty:
            return {}
        df["year"] = df["year"].astype(int)
        df["month"] = df["month"].astype(int)
        df["water_area"] = df["water_area"].astype(float)
        return self._split_by_hylak_id(df)

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        try:
            df = self._client.query_df(
                "SELECT * FROM anomaly WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception:
            return {}
        return self._frozen_from_anomaly_df(df)

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        if not id_list:
            return {}
        placeholders = ",".join("?" for _ in id_list)
        try:
            df = self._client.query_df(
                f"SELECT * FROM anomaly WHERE hylak_id IN ({placeholders})",
                parameters=id_list,
            )
        except Exception:
            return {}
        return self._frozen_from_anomaly_df(df)

    def fetch_max_hylak_id(self) -> int:
        try:
            df = self._client.query_df("SELECT MAX(hylak_id) AS max_id FROM lake_info")
            val = df.iloc[0, 0]
            return int(val) if val is not None else 0
        except Exception:
            return 0

    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
        status_table = f"{algorithm}_run_status"
        try:
            df = self._client.query_df(
                f"SELECT DISTINCT hylak_id FROM {status_table} "
                f"WHERE hylak_id >= ? AND hylak_id < ? AND status = 'done'",
                parameters=[chunk_start, chunk_end],
            )
            return set(df["hylak_id"].astype(int))
        except Exception:
            return set()


class ParquetBatchWriter(_ParquetBatchBase, BatchWriter):
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        if not any(rows_by_table.values()):
            return
        for table_name, rows in rows_by_table.items():
            if not rows:
                continue
            new_df = pd.DataFrame(rows)
            if table_name in _APPEND_ONLY_TABLES:
                target_dir = self._data_dir / table_name
                target_dir.mkdir(parents=True, exist_ok=True)
                part_path = target_dir / f"part-{uuid4().hex}.parquet"
                new_df.to_parquet(part_path, index=False)
                self._client.register_or_replace_glob(table_name, str(target_dir / "*.parquet"))
                log.info("Appended %d rows to %s", len(rows), part_path)
                continue

            parquet_path = self._data_dir / f"{table_name}.parquet"
            if parquet_path.exists():
                existing_df = pd.read_parquet(parquet_path)
                new_df = pd.concat([existing_df, new_df], ignore_index=True)
            new_df.to_parquet(parquet_path, index=False)
            self._client.register_or_replace(table_name, parquet_path)
            log.info("Persisted %d rows to %s (total: %d)", len(rows), parquet_path, len(new_df))

    def ensure_schema(self, algorithm: str) -> None:
        return None


def build_batch_reader(config: SourceConfig | None = None) -> BatchReader:
    config = config or SourceConfig()
    if config.backend == Backend.POSTGRES:
        return PostgresBatchReader(config)
    if config.backend == Backend.PARQUET:
        return ParquetBatchReader(config)
    raise ValueError(f"Unsupported backend for batch reader: {config.backend!r}")


def build_batch_writer(config: SourceConfig | None = None) -> BatchWriter:
    config = config or SourceConfig()
    if config.backend == Backend.POSTGRES:
        return PostgresBatchWriter(config)
    if config.backend == Backend.PARQUET:
        return ParquetBatchWriter(config)
    raise ValueError(f"Unsupported backend for batch writer: {config.backend!r}")
