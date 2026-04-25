"""ParquetLakeProvider: read-only implementation via DuckDB."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.parquet.client import DuckDBClient
from lakesource.table_config import TableConfig

from .base import LakeProvider

log = logging.getLogger(__name__)


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
            "SELECT * FROM area_quality WHERE hylak_id >= ? AND hylak_id < ?",
            parameters=[chunk_start, chunk_end],
        )
        return self._split_by_hylak_id(df)

    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
        if not id_list:
            return {}
        placeholders = ",".join("?" for _ in id_list)
        df = self._client.query_df(
            f"SELECT * FROM area_quality WHERE hylak_id IN ({placeholders})",
            parameters=id_list,
        )
        return self._split_by_hylak_id(df)

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
        result: dict[int, set[int]] = {}
        if df.empty:
            return result
        for hid, group in df.groupby("hylak_id"):
            frozen = set()
            for _, row in group.iterrows():
                if "year" in row and "month" in row:
                    frozen.add(int(row["year"]) * 100 + int(row["month"]))
            result[int(hid)] = frozen
        return result

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
        result: dict[int, set[int]] = {}
        if df.empty:
            return result
        for hid, group in df.groupby("hylak_id"):
            frozen = set()
            for _, row in group.iterrows():
                if "year" in row and "month" in row:
                    frozen.add(int(row["year"]) * 100 + int(row["month"]))
            result[int(hid)] = frozen
        return result

    def fetch_max_hylak_id(self) -> int:
        try:
            df = self._client.query_df("SELECT MAX(hylak_id) AS max_id FROM area_quality")
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

    # ------------------------------------------------------------------
    # Aggregation reads (lakeviz global maps)
    # ------------------------------------------------------------------

    def fetch_extremes_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        cache = self._cache_path("quantile", f"extremes_grid_agg_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._grid_agg(
                "quantile_extremes", resolution
            )
        )

    def fetch_extremes_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        cache = self._cache_path("quantile", f"extremes_by_type_grid_agg_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._grid_agg(
                "quantile_extremes", resolution, group_cols=["event_type"]
            )
        )

    def fetch_transitions_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        cache = self._cache_path("quantile", f"transitions_grid_agg_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._grid_agg(
                "quantile_abrupt_transitions", resolution
            )
        )

    def fetch_transitions_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        cache = self._cache_path("quantile", f"transitions_by_type_grid_agg_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._grid_agg(
                "quantile_abrupt_transitions", resolution, group_cols=["transition_type"]
            )
        )

    def fetch_eot_convergence_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        q_tag = f"{tail}_q{threshold_quantile:.4f}"
        cache = self._cache_path("eot", f"eot_convergence_{q_tag}_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._eot_grid_agg(
                "eot_results", resolution, tail, threshold_quantile,
                value_col="CASE WHEN converged THEN 1.0 ELSE 0.0 END",
                value_alias="convergence_rate",
            )
        )

    def fetch_eot_converged_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        q_tag = f"{tail}_q{threshold_quantile:.4f}"
        cache = self._cache_path("eot", f"eot_converged_{q_tag}_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._eot_grid_agg(
                "eot_results", resolution, tail, threshold_quantile,
                value_col="converged",
                value_alias="converged_count",
                agg="SUM",
                extra_where="AND converged = true",
            )
        )

    def fetch_pwm_convergence_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        cache = self._cache_path("pwm_extreme", f"convergence_grid_agg_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._pwm_grid_agg(
                "pwm_extreme_thresholds", resolution,
                value_col="CASE WHEN converged THEN 1.0 ELSE 0.0 END",
                value_alias="convergence_rate",
            )
        )

    def fetch_pwm_converged_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        cache = self._cache_path("pwm_extreme", f"converged_grid_agg_r{resolution}.parquet")
        return self._cached_or_compute(
            cache, refresh, lambda: self._pwm_grid_agg(
                "pwm_extreme_thresholds", resolution,
                value_col="converged",
                value_alias="converged_count",
                agg="SUM",
                extra_where="AND converged = true",
            )
        )

    # ------------------------------------------------------------------
    # Writes (not supported)
    # ------------------------------------------------------------------

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        raise NotImplementedError("ParquetLakeProvider is read-only; use PostgresLakeProvider for writes")

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
