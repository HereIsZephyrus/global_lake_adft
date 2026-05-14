"""ParquetLakeProvider: backend reads and grid aggregations via DuckDB."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.parquet.client import DuckDBClient

from .base import LakeProvider

log = logging.getLogger(__name__)


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
        except Exception as e:
            log.warning("fetch_frozen_year_months_chunk failed: %s", e)
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
        except Exception as e:
            log.warning("fetch_frozen_year_months_by_ids failed: %s", e)
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
        except Exception as e:
            log.warning("fetch_atlas_area_chunk failed: %s", e)
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
        except Exception as e:
            log.warning("fetch_atlas_area_by_ids failed: %s", e)
            return {}
        if df.empty:
            return {}
        return {
            int(row.hylak_id): float(row.lake_area) if row.lake_area is not None else 0.0
            for row in df.itertuples(index=False)
        }

    def fetch_done_ids(
        self,
        table_name: str,
        chunk_start: int,
        chunk_end: int,
        *,
        status: str | None = None,
    ) -> set[int]:
        df = self._read_table_df(table_name)
        if df.empty or "hylak_id" not in df.columns:
            return set()
        mask = (df["hylak_id"] >= chunk_start) & (df["hylak_id"] < chunk_end)
        if status is not None and "status" in df.columns:
            mask &= df["status"] == status
        return set(df.loc[mask, "hylak_id"].astype(int).tolist())

    def fetch_seasonal_amplitude_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float | None]:
        try:
            df = self._client.query_df(
                "SELECT hylak_id, annual_means_std, mean_area FROM lake_info WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception as e:
            log.warning("fetch_seasonal_amplitude_chunk failed: %s", e)
            return {}
        result: dict[int, float | None] = {}
        for row in df.itertuples(index=False):
            if row.annual_means_std is not None and row.mean_area is not None and row.mean_area > 0:
                result[int(row.hylak_id)] = float(row.annual_means_std) / float(row.mean_area)
            else:
                result[int(row.hylak_id)] = None
        return result

    def fetch_seasonal_amplitude_by_ids(self, id_list: list[int]) -> dict[int, float | None]:
        if not id_list:
            return {}
        placeholders = ",".join("?" for _ in id_list)
        try:
            df = self._client.query_df(
                f"SELECT hylak_id, annual_means_std, mean_area FROM lake_info WHERE hylak_id IN ({placeholders})",
                parameters=id_list,
            )
        except Exception as e:
            log.warning("fetch_seasonal_amplitude_by_ids failed: %s", e)
            return {}
        result: dict[int, float | None] = {}
        for row in df.itertuples(index=False):
            if row.annual_means_std is not None and row.mean_area is not None and row.mean_area > 0:
                result[int(row.hylak_id)] = float(row.annual_means_std) / float(row.mean_area)
            else:
                result[int(row.hylak_id)] = None
        return result

    def ensure_table(self, table_name: str) -> None:
        del table_name
        return None

    def truncate_table(self, table_name: str) -> None:
        table_path = self._table_path(table_name)
        if table_path.exists():
            table_path.unlink()
        table_dir = self._data_dir / table_name
        if table_dir.exists() and table_dir.is_dir():
            for child in table_dir.glob("*.parquet"):
                child.unlink()

    def upsert_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self._upsert_table(table_name, rows)

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict[str, Any]]:
        df = self._read_table_df(table_name)
        if df.empty or "hylak_id" not in df.columns:
            return []
        filtered = df[(df["hylak_id"] >= chunk_start) & (df["hylak_id"] < chunk_end)]
        if filtered.empty:
            return []
        return filtered.to_dict("records")

    def delete_ids(self, table_name: str, hylak_ids: list[int]) -> None:
        if not hylak_ids:
            return
        df = self._read_table_df(table_name)
        if df.empty or "hylak_id" not in df.columns:
            return
        new_df = df[~df["hylak_id"].isin(hylak_ids)].reset_index(drop=True)
        self._write_table_df(table_name, new_df)

    def fetch_area_statuses(self) -> dict[int, tuple[str, int]]:
        result: dict[int, tuple[str, int]] = {}
        quality_df = self._read_table_df("area_quality")
        if not quality_df.empty and "hylak_id" in quality_df.columns:
            for hid in quality_df["hylak_id"].astype(int).tolist():
                result[hid] = ("quality", 0)
        anomalies_df = self._read_table_df("area_anomalies")
        if not anomalies_df.empty and {"hylak_id", "anomaly_flags"}.issubset(anomalies_df.columns):
            for row in anomalies_df[["hylak_id", "anomaly_flags"]].itertuples(index=False):
                result[int(row.hylak_id)] = ("anomalies", int(row.anomaly_flags))
        return result

    def fetch_zero_quantile_flags(self) -> dict[int, int]:
        result: dict[int, int] = {}
        df = self._read_table_df("area_anomalies")
        if df.empty or not {"hylak_id", "anomaly_flags"}.issubset(df.columns):
            return result
        flagged = df[df["anomaly_flags"].astype(int).map(lambda value: value & 1 > 0)]
        for row in flagged[["hylak_id", "anomaly_flags"]].itertuples(index=False):
            result[int(row.hylak_id)] = int(row.anomaly_flags)
        return result

    def clear_zero_quantile_flag(self, hylak_ids: list[int]) -> int:
        if not hylak_ids:
            return 0
        df = self._read_table_df("area_anomalies")
        if df.empty or not {"hylak_id", "anomaly_flags"}.issubset(df.columns):
            return 0
        mask = df["hylak_id"].isin(hylak_ids)
        df.loc[mask, "anomaly_flags"] = df.loc[mask, "anomaly_flags"].astype(int).map(lambda value: value & ~1)
        updated = int(mask.sum())
        self._write_table_df("area_anomalies", df)
        return updated

    def find_nonzero_quantile_lakes(self, hylak_ids: list[int], quantile: float) -> set[int]:
        if not hylak_ids:
            return set()
        lake_frames = self.fetch_lake_area_by_ids(hylak_ids)
        frozen_map = self.fetch_frozen_year_months_by_ids(hylak_ids)
        result: set[int] = set()
        for hylak_id, df in lake_frames.items():
            frozen = frozen_map.get(hylak_id, set())
            if frozen and not df.empty:
                mask = ~((df["year"].astype(int) * 100 + df["month"].astype(int)).isin(frozen))
                filtered = df.loc[mask]
            else:
                filtered = df
            if filtered.empty:
                continue
            if float(filtered["water_area"].quantile(quantile)) > 0:
                result.add(int(hylak_id))
        return result

    def update_area_anomaly_flags(self, updates: list[tuple[int, int]]) -> None:
        if not updates:
            return
        df = self._read_table_df("area_anomalies")
        if df.empty or not {"hylak_id", "anomaly_flags"}.issubset(df.columns):
            return
        update_map = {int(hid): int(flags) for hid, flags in updates}
        df.loc[df["hylak_id"].isin(update_map), "anomaly_flags"] = df.loc[
            df["hylak_id"].isin(update_map), "hylak_id"
        ].map(update_map)
        self._write_table_df("area_anomalies", df)

    def fetch_impact_pairs(self) -> list[dict[str, int]]:
        try:
            df = self._client.query_df(
                """
                SELECT a.hylak_id, a.nearest_id, a.topo_level
                FROM af_nearest a
                LEFT JOIN area_anomalies aa ON aa.hylak_id = a.hylak_id
                WHERE a.topo_level > 8 AND a.nearest_id IS NOT NULL AND aa.hylak_id IS NULL
                ORDER BY a.hylak_id
                """
            )
        except Exception as e:
            log.warning("fetch_impact_pairs failed: %s", e)
            return []
        return [
            {
                "hylak_id": int(row.hylak_id),
                "nearest_id": int(row.nearest_id),
                "topo_level": int(row.topo_level),
            }
            for row in df.itertuples(index=False)
        ]

    def fetch_lake_centroids_chunk(self, chunk_start: int, chunk_end: int) -> list[tuple[int, str]]:
        raise NotImplementedError("Pfaf lookup requires PostGIS; use PostgresLakeProvider")

    def lookup_pfaf_chunk(self, centroids: list[tuple[int, str]]) -> dict[int, int | None]:
        raise NotImplementedError("Pfaf lookup requires PostGIS; use PostgresLakeProvider")

    def fetch_type1_lake_records(self) -> list[dict[str, int | float | None]]:
        raise NotImplementedError("Nearest-natural search requires centroid geometry; use PostgresLakeProvider")

    def fetch_non_type1_lake_records(
        self, limit_id: int | None = None
    ) -> list[dict[str, int | float | None]]:
        raise NotImplementedError("Nearest-natural search requires centroid geometry; use PostgresLakeProvider")

    def fetch_max_hylak_id(self) -> int:
        try:
            df = self._client.query_df("SELECT MAX(hylak_id) AS max_id FROM lake_info")
            val = df.iloc[0, 0]
            return int(val) if val is not None else 0
        except Exception as e:
            log.warning("fetch_max_hylak_id failed: %s", e)
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

    def _table_path(self, table_name: str) -> Path:
        suffix = ""
        if table_name not in self._tc.suffix_exempt:
            suffix = os.environ.get("PARQUET_TABLE_SUFFIX", "")
        return self._data_dir / f"{table_name}{suffix}.parquet"

    def _read_table_df(self, table_name: str) -> pd.DataFrame:
        table_path = self._table_path(table_name)
        if table_path.exists():
            return pd.read_parquet(table_path)
        return pd.DataFrame()

    def _write_table_df(self, table_name: str, df: pd.DataFrame) -> None:
        table_path = self._table_path(table_name)
        table_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(table_path, index=False)
        self._client.register_or_replace(table_name, table_path)

    def _upsert_table(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        new_df = pd.DataFrame(rows)
        if new_df.empty:
            return
        existing_df = self._read_table_df(table_name)
        if existing_df.empty:
            merged = new_df
        elif "hylak_id" in new_df.columns:
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            merged = combined.drop_duplicates(subset=["hylak_id"], keep="last").reset_index(drop=True)
        else:
            merged = pd.concat([existing_df, new_df], ignore_index=True)
        self._write_table_df(table_name, merged)

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
