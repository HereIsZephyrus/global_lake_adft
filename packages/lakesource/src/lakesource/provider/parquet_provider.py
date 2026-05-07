"""ParquetLakeProvider: read-only implementation via DuckDB."""

from __future__ import annotations

import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.parquet.client import DuckDBClient
from lakesource.table_config import TableConfig

from .base import LakeProvider

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
    "area_entropy_cv",
    "area_quality",
    "area_anomalies",
}


def _ensure_queries_registered() -> None:
    from lakesource.provider.grid_query import list_grid_queries
    if not list_grid_queries():
        import lakesource.quantile.grid_queries  # noqa: F401
        import lakesource.pwm_extreme.grid_queries  # noqa: F401
        import lakesource.eot.grid_queries  # noqa: F401
        import lakesource.comparison.grid_queries  # noqa: F401


class ParquetLakeProvider(LakeProvider):
    def __init__(self, config: SourceConfig) -> None:
        if config.data_dir is None:
            raise ValueError("data_dir is required for ParquetLakeProvider")
        self._config = config
        self._tc = config.t
        self._data_dir = Path(config.data_dir)
        self._client = DuckDBClient(data_dir=self._data_dir, table_config=self._tc)
        self._cache_dir = self._data_dir.parent / "cache"

    # ------------------------------------------------------------------
    # Core reads
    # ------------------------------------------------------------------

    def fetch_lake_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, pd.DataFrame]:
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

    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
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

    def _frozen_from_anomaly_df(self, df: pd.DataFrame) -> dict[int, set[int]]:
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

    def fetch_frozen_year_months_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, set[int]]:
        try:
            df = self._client.query_df(
                "SELECT * FROM anomaly WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception:
            return {}
        return self._frozen_from_anomaly_df(df)

    def fetch_frozen_year_months_by_ids(
        self, id_list: list[int]
    ) -> dict[int, set[int]]:
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

    def fetch_atlas_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float]:
        try:
            df = self._client.query_df(
                "SELECT hylak_id, lake_area FROM lake_info WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception:
            return {}
        if df.empty:
            return {}
        return {
            int(row.hylak_id): float(row.lake_area) if row.lake_area is not None else 0.0
            for row in df.itertuples(index=False)
        }

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        if not id_list:
            return {}
        placeholders = ",".join("?" for _ in id_list)
        try:
            df = self._client.query_df(
                f"SELECT hylak_id, lake_area FROM lake_info WHERE hylak_id IN ({placeholders})",
                parameters=id_list,
            )
        except Exception:
            return {}
        if df.empty:
            return {}
        return {
            int(row.hylak_id): float(row.lake_area) if row.lake_area is not None else 0.0
            for row in df.itertuples(index=False)
        }

    def fetch_max_hylak_id(self) -> int:
        try:
            df = self._client.query_df("SELECT MAX(hylak_id) AS max_id FROM lake_info")
            val = df.iloc[0, 0]
            return int(val) if val is not None else 0
        except Exception:
            return 0

    def fetch_lake_geometry_wkt_by_ids(
        self,
        hylak_ids: list[int],
        *,
        simplify_tolerance_meters: float | None = None,
    ) -> pd.DataFrame:
        raise NotImplementedError(
            "Lake geometry WKT requires PostGIS; use PostgresLakeProvider"
        )

    # ------------------------------------------------------------------
    # Algorithm-specific reads
    # ------------------------------------------------------------------

    def fetch_done_ids(
        self, algorithm: str, chunk_start: int, chunk_end: int
    ) -> set[int]:
        if algorithm == "quality":
            done_ids: set[int] = set()
            try:
                quality_df = self._client.query_df(
                    "SELECT DISTINCT hylak_id FROM area_quality WHERE hylak_id >= ? AND hylak_id < ?",
                    parameters=[chunk_start, chunk_end],
                )
                done_ids |= set(quality_df["hylak_id"].astype(int))
            except Exception:
                pass
            try:
                anomaly_df = self._client.query_df(
                    "SELECT DISTINCT hylak_id FROM area_anomalies WHERE hylak_id >= ? AND hylak_id < ?",
                    parameters=[chunk_start, chunk_end],
                )
                done_ids |= set(anomaly_df["hylak_id"].astype(int))
            except Exception:
                pass
            return done_ids
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

    def count_done_ids(
        self, algorithm: str, chunk_start: int, chunk_end: int
    ) -> int:
        if algorithm == "quality":
            return len(self.fetch_done_ids(algorithm, chunk_start, chunk_end))
        status_table = f"{algorithm}_run_status"
        try:
            df = self._client.query_df(
                f"SELECT COUNT(DISTINCT hylak_id) AS cnt FROM {status_table} "
                f"WHERE hylak_id >= ? AND hylak_id < ? AND status = 'done'",
                parameters=[chunk_start, chunk_end],
            )
            return int(df.iloc[0, 0])
        except Exception:
            return 0

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
        return query.fetch_parquet(
            self._client, self._cache_dir, resolution, refresh=refresh, **kwargs
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Schema management (no-op for parquet)
    # ------------------------------------------------------------------

    def ensure_schema(self, algorithm: str) -> None:
        pass

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    def backend_name(self) -> str:
        return "parquet"

    @property
    def cache_dir(self) -> Path | None:
        return self._cache_dir

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_by_hylak_id(df: pd.DataFrame) -> dict[int, pd.DataFrame]:
        if df.empty:
            return {}
        result: dict[int, pd.DataFrame] = {}
        for hid, group in df.groupby("hylak_id"):
            result[int(hid)] = group.drop(columns=["hylak_id"]).reset_index(drop=True)
        return result

    def _cache_path(self, sub_dir: str, filename: str) -> Path:
        p = self._cache_dir / sub_dir / filename
        p.parent.mkdir(parents=True, exist_ok=True)
        return p

    def _cached_or_compute(
        self, cache_path: Path, refresh: bool, compute_fn
    ) -> pd.DataFrame:
        if not refresh and cache_path.exists():
            log.info("Loading from cache: %s", cache_path)
            return pd.read_parquet(cache_path)
        df = compute_fn()
        df.to_parquet(cache_path, index=False)
        log.info("Cached %d rows to %s", len(df), cache_path)
        return df

    def _grid_agg(
        self,
        table: str,
        resolution: float,
        group_cols: list[str] | None = None,
    ) -> pd.DataFrame:
        extra_select = ""
        extra_group = ""
        if group_cols:
            cols = ", ".join(group_cols)
            extra_select = f"e.{cols}, "
            extra_group = f", {', '.join(f'e.{c}' for c in group_cols)}"

        sql = f"""
        SELECT {extra_select}
               FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT e.hylak_id)                    AS lake_count,
               COUNT(*)                                       AS event_count
        FROM   {table} e
        JOIN   lake_info l ON l.hylak_id = e.hylak_id
        GROUP BY {extra_group} 2, 3
        ORDER BY {extra_group} 2, 3
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        for col in ("lake_count", "event_count"):
            if col in df.columns:
                df[col] = df[col].astype(int)
        return df

    def _eot_grid_agg(
        self,
        table: str,
        resolution: float,
        tail: str,
        threshold_quantile: float,
        value_col: str,
        value_alias: str,
        agg: str = "AVG",
        extra_where: str = "",
    ) -> pd.DataFrame:
        sql = f"""
        SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT r.hylak_id)                    AS lake_count,
               {agg}({value_col})                             AS {value_alias}
        FROM   {table} r
        JOIN   lake_info l ON l.hylak_id = r.hylak_id
        WHERE  r.tail = '{tail}'
          AND  r.threshold_quantile = '{threshold_quantile}'
          {extra_where}
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df

    def _pwm_grid_agg(
        self,
        table: str,
        resolution: float,
        value_col: str,
        value_alias: str,
        agg: str = "AVG",
        extra_where: str = "",
    ) -> pd.DataFrame:
        sql = f"""
        SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT t.hylak_id)                    AS lake_count,
               {agg}({value_col})                             AS {value_alias}
        FROM   {table} t
        JOIN   lake_info l ON l.hylak_id = t.hylak_id
        WHERE  1=1
          {extra_where}
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df

    def _pwm_converged_grid_agg(self, resolution: float) -> pd.DataFrame:
        sql = f"""
        SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT t.hylak_id)                    AS lake_count,
               MEDIAN(t.threshold_high)                      AS median_threshold_high,
               MEDIAN(t.threshold_low)                       AS median_threshold_low
        FROM   pwm_extreme_thresholds t
        JOIN   lake_info l ON l.hylak_id = t.hylak_id
        WHERE  t.converged = true
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df

    def _pwm_monthly_grid_agg(self, resolution: float) -> pd.DataFrame:
        sql = f"""
        SELECT t.month,
               FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT t.hylak_id)                    AS lake_count,
               MEDIAN(t.threshold_high)                      AS median_threshold_high,
               MEDIAN(t.threshold_low)                       AS median_threshold_low
        FROM   pwm_extreme_thresholds t
        JOIN   lake_info l ON l.hylak_id = t.hylak_id
        WHERE  t.converged = true
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        df["month"] = df["month"].astype(int)
        return df

    @staticmethod
    def _crossent_threshold_sql(p: float, direction: str) -> str:
        u = 1.0 - p if direction == "high" else p
        ln_arg = 1.0 - u
        return f"""t.mean_area * (
               (t.epsilon - (1 - t.epsilon) * LN({ln_arg}))
               * EXP(-(t.lambda_0
                     + t.lambda_1 * {u}
                     + t.lambda_2 * {u} * {u}
                     + t.lambda_3 * {u} * {u} * {u}
                     + t.lambda_4 * {u} * {u} * {u} * {u}))
           )"""

    def _pwm_exceedance_grid_agg(
        self, resolution: float, p_high: float, p_low: float,
    ) -> pd.DataFrame:
        th_high = self._crossent_threshold_sql(p_high, "high")
        th_low = self._crossent_threshold_sql(p_low, "low")
        sql = f"""
        WITH deduped_area AS (
            SELECT DISTINCT hylak_id, year_month, water_area
            FROM lake_area
        ),
        quantile_thresholds AS (
            SELECT t.hylak_id, t.month,
                   {th_high} AS threshold_high,
                   {th_low}  AS threshold_low
            FROM   pwm_extreme_thresholds t
            WHERE  t.converged = true
        ),
        exceedance AS (
            SELECT la.hylak_id,
                   SUM(CASE WHEN la.water_area >= qt.threshold_high THEN 1 ELSE 0 END) AS high_count,
                   SUM(CASE WHEN la.water_area <= qt.threshold_low  THEN 1 ELSE 0 END) AS low_count
            FROM   deduped_area la
            JOIN   quantile_thresholds qt
              ON   qt.hylak_id = la.hylak_id
              AND  qt.month = MONTH(la.year_month)
            GROUP BY la.hylak_id
        )
        SELECT FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT e.hylak_id)                 AS lake_count,
               AVG(e.high_count)                           AS mean_high_exceedance,
               AVG(e.low_count)                            AS mean_low_exceedance,
               MEDIAN(e.high_count)                        AS median_high_exceedance,
               MEDIAN(e.low_count)                         AS median_low_exceedance
        FROM   exceedance e
        JOIN   lake_info l ON l.hylak_id = e.hylak_id
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        return df

    def _pwm_monthly_exceedance_grid_agg(
        self, resolution: float, p_high: float, p_low: float,
    ) -> pd.DataFrame:
        th_high = self._crossent_threshold_sql(p_high, "high")
        th_low = self._crossent_threshold_sql(p_low, "low")
        sql = f"""
        WITH deduped_area AS (
            SELECT DISTINCT hylak_id, year_month, water_area
            FROM lake_area
        ),
        quantile_thresholds AS (
            SELECT t.hylak_id, t.month,
                   {th_high} AS threshold_high,
                   {th_low}  AS threshold_low
            FROM   pwm_extreme_thresholds t
            WHERE  t.converged = true
        )
        SELECT qt.month,
               FLOOR(l.lat / {resolution}) * {resolution} AS cell_lat,
               FLOOR(l.lon / {resolution}) * {resolution} AS cell_lon,
               COUNT(DISTINCT la.hylak_id)                AS lake_count,
               AVG(CASE WHEN la.water_area >= qt.threshold_high THEN 1.0 ELSE 0.0 END) AS high_exceedance_rate,
               AVG(CASE WHEN la.water_area <= qt.threshold_low  THEN 1.0 ELSE 0.0 END) AS low_exceedance_rate
        FROM   deduped_area la
        JOIN   quantile_thresholds qt
          ON   qt.hylak_id = la.hylak_id
          AND  qt.month = MONTH(la.year_month)
        JOIN   lake_info l ON l.hylak_id = la.hylak_id
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3
        """
        df = self._client.query_df(sql)
        for col in ("cell_lat", "cell_lon"):
            if col in df.columns:
                df[col] = df[col].astype(float)
        df["month"] = df["month"].astype(int)
        return df
