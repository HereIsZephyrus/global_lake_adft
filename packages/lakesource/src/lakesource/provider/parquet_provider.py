"""ParquetLakeProvider: read-only parquet access via on-demand DuckDB views."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from lakesource.config import SourceConfig
from lakesource.parquet.client import DuckDBClient

from .base import LakeProvider

log = logging.getLogger(__name__)


def _ensure_queries_registered() -> None:
    from lakesource.provider.grid_query import list_grid_queries

    if not list_grid_queries():
        import lakesource.comparison.grid_queries  # noqa: F401  # pylint: disable=unused-import
        import lakesource.eot.grid_queries  # noqa: F401
        import lakesource.pwm.grid_queries  # noqa: F401
        import lakesource.quantile.grid_queries  # noqa: F401


class ParquetLakeProvider(LakeProvider):
    """Read-only parquet provider backed by lazily-registered DuckDB views."""

    def __init__(self, config: SourceConfig) -> None:
        if config.data_dir is None:
            raise ValueError("data_dir is required for ParquetLakeProvider")
        self._config = config
        self._data_dir = Path(config.data_dir)
        self._client = DuckDBClient(data_dir=self._data_dir, table_config=self._config.t)
        self._cache_dir = self._data_dir.parent / "cache"

    def _ensure_view(self, table_name: str, *, root: Path | None = None) -> bool:
        return self._client.ensure_registered(table_name, data_dir=root or self._data_dir)

    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, pd.DataFrame]:
        if not self._ensure_view("lake_area"):
            return {}
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
        if not id_list or not self._ensure_view("lake_area"):
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
            frozen = {
                int(row["year"]) * 100 + int(row["month"])
                for _, row in group.iterrows()
                if "year" in row and "month" in row
            }
            result[int(hid)] = frozen
        return result

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, set[int]]:
        if not self._ensure_view("anomaly"):
            return {}
        try:
            df = self._client.query_df(
                "SELECT * FROM anomaly WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception as exc:
            log.warning("fetch_frozen_year_months_chunk failed: %s", exc)
            return {}
        return self._frozen_from_anomaly_df(df)

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]) -> dict[int, set[int]]:
        if not id_list or not self._ensure_view("anomaly"):
            return {}
        placeholders = ",".join("?" for _ in id_list)
        try:
            df = self._client.query_df(
                f"SELECT * FROM anomaly WHERE hylak_id IN ({placeholders})",
                parameters=id_list,
            )
        except Exception as exc:
            log.warning("fetch_frozen_year_months_by_ids failed: %s", exc)
            return {}
        return self._frozen_from_anomaly_df(df)

    def fetch_atlas_area_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, float]:
        if not self._ensure_view("lake_info"):
            return {}
        try:
            df = self._client.query_df(
                "SELECT hylak_id, lake_area FROM lake_info WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception as exc:
            log.warning("fetch_atlas_area_chunk failed: %s", exc)
            return {}
        if df.empty:
            return {}
        return {
            int(row.hylak_id): float(row.lake_area) if row.lake_area is not None else 0.0
            for row in df.itertuples(index=False)
        }

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        if not id_list or not self._ensure_view("lake_info"):
            return {}
        placeholders = ",".join("?" for _ in id_list)
        try:
            df = self._client.query_df(
                f"SELECT hylak_id, lake_area FROM lake_info WHERE hylak_id IN ({placeholders})",
                parameters=id_list,
            )
        except Exception as exc:
            log.warning("fetch_atlas_area_by_ids failed: %s", exc)
            return {}
        if df.empty:
            return {}
        return {
            int(row.hylak_id): float(row.lake_area) if row.lake_area is not None else 0.0
            for row in df.itertuples(index=False)
        }

    def fetch_done_ids(self, table_name: str, chunk_start: int, chunk_end: int, *, status: str | None = None) -> set[int]:
        if not self._ensure_view(table_name):
            return set()
        df = self._read_table_df(table_name)
        if df.empty or "hylak_id" not in df.columns:
            return set()
        mask = (df["hylak_id"] >= chunk_start) & (df["hylak_id"] < chunk_end)
        if status is not None and "status" in df.columns:
            mask &= df["status"] == status
        return set(df.loc[mask, "hylak_id"].astype(int).tolist())

    def fetch_seasonal_amplitude_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, float | None]:
        if not self._ensure_view("lake_info"):
            return {}
        try:
            df = self._client.query_df(
                "SELECT hylak_id, annual_means_std, mean_area FROM lake_info WHERE hylak_id >= ? AND hylak_id < ?",
                parameters=[chunk_start, chunk_end],
            )
        except Exception as exc:
            log.warning("fetch_seasonal_amplitude_chunk failed: %s", exc)
            return {}
        result: dict[int, float | None] = {}
        for row in df.itertuples(index=False):
            if row.annual_means_std is not None and row.mean_area is not None and row.mean_area > 0:
                result[int(row.hylak_id)] = float(row.annual_means_std) / float(row.mean_area)
            else:
                result[int(row.hylak_id)] = None
        return result

    def fetch_seasonal_amplitude_by_ids(self, id_list: list[int]) -> dict[int, float | None]:
        if not id_list or not self._ensure_view("lake_info"):
            return {}
        placeholders = ",".join("?" for _ in id_list)
        try:
            df = self._client.query_df(
                f"SELECT hylak_id, annual_means_std, mean_area FROM lake_info WHERE hylak_id IN ({placeholders})",
                parameters=id_list,
            )
        except Exception as exc:
            log.warning("fetch_seasonal_amplitude_by_ids failed: %s", exc)
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
        raise NotImplementedError("ParquetLakeProvider is read-only; writes are managed externally")

    def truncate_table(self, table_name: str) -> None:
        del table_name
        raise NotImplementedError("ParquetLakeProvider is read-only; writes are managed externally")

    def upsert_rows(self, table_name: str, rows: list[dict]) -> None:
        del table_name, rows
        raise NotImplementedError("ParquetLakeProvider is read-only; writes are managed externally")

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict]:
        if not self._ensure_view(table_name):
            return []
        df = self._read_table_df(table_name)
        if df.empty:
            try:
                df = self._client.query_df(
                    f"SELECT * FROM {table_name} WHERE hylak_id >= ? AND hylak_id < ?",
                    parameters=[chunk_start, chunk_end],
                )
            except Exception:
                df = pd.DataFrame()
        if df.empty or "hylak_id" not in df.columns:
            return []
        filtered = df[(df["hylak_id"] >= chunk_start) & (df["hylak_id"] < chunk_end)]
        return filtered.to_dict("records") if not filtered.empty else []

    def delete_ids(self, table_name: str, hylak_ids: list[int]) -> None:
        del table_name, hylak_ids
        raise NotImplementedError("ParquetLakeProvider is read-only; writes are managed externally")

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
        del hylak_ids
        raise NotImplementedError("ParquetLakeProvider is read-only; writes are managed externally")

    def find_nonzero_quantile_lakes(self, hylak_ids: list[int], quantile: float) -> set[int]:
        if not hylak_ids:
            return set()
        lake_frames = self.fetch_lake_area_by_ids(hylak_ids)
        frozen_map = self.fetch_frozen_year_months_by_ids(hylak_ids)
        result: set[int] = set()
        for hylak_id, df in lake_frames.items():
            frozen = frozen_map.get(hylak_id, set())
            filtered = df.loc[~((df["year"].astype(int) * 100 + df["month"].astype(int)).isin(frozen))] if frozen and not df.empty else df
            if not filtered.empty and float(filtered["water_area"].quantile(quantile)) > 0:
                result.add(int(hylak_id))
        return result

    def update_area_anomaly_flags(self, updates: list[tuple[int, int]]) -> None:
        del updates
        raise NotImplementedError("ParquetLakeProvider is read-only; writes are managed externally")

    def fetch_impact_pairs(self) -> list[dict[str, int]]:
        if not self._ensure_view("af_nearest") or not self._ensure_view("area_anomalies"):
            return []
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
        except Exception as exc:
            log.warning("fetch_impact_pairs failed: %s", exc)
            return []
        return [
            {"hylak_id": int(row.hylak_id), "nearest_id": int(row.nearest_id), "topo_level": int(row.topo_level)}
            for row in df.itertuples(index=False)
        ]

    def fetch_lake_centroids_chunk(self, chunk_start: int, chunk_end: int) -> list[tuple[int, str]]:
        del chunk_start, chunk_end
        raise NotImplementedError("Pfaf lookup requires PostGIS; use PostgresLakeProvider")

    def lookup_pfaf_chunk(self, centroids: list[tuple[int, str]]) -> dict[int, int | None]:
        del centroids
        raise NotImplementedError("Pfaf lookup requires PostGIS; use PostgresLakeProvider")

    def fetch_type1_lake_records(self) -> list[dict[str, int | float | None]]:
        raise NotImplementedError("Nearest-natural search requires centroid geometry; use PostgresLakeProvider")

    def fetch_non_type1_lake_records(self, limit_id: int | None = None) -> list[dict[str, int | float | None]]:
        del limit_id
        raise NotImplementedError("Nearest-natural search requires centroid geometry; use PostgresLakeProvider")

    def fetch_max_hylak_id(self) -> int:
        if not self._ensure_view("lake_info"):
            return 0
        try:
            df = self._client.query_df("SELECT MAX(hylak_id) AS max_id FROM lake_info")
            val = df.iloc[0, 0]
            return int(val) if val is not None else 0
        except Exception as exc:
            log.warning("fetch_max_hylak_id failed: %s", exc)
            return 0

    def fetch_lake_geometry_wkt_by_ids(self, hylak_ids: list[int], *, simplify_tolerance_meters: float | None = None) -> pd.DataFrame:
        del hylak_ids, simplify_tolerance_meters
        raise NotImplementedError("Lake geometry WKT requires PostGIS; use PostgresLakeProvider")

    def fetch_grid_agg(self, query_name: str, resolution: float = 0.5, *, refresh: bool = False, **kwargs) -> pd.DataFrame:
        from lakesource.provider.grid_query import get_grid_query

        _ensure_queries_registered()
        query = get_grid_query(query_name)
        self._register_grid_dependencies()
        return query.fetch_parquet(self._client, self._cache_dir, resolution, refresh=refresh, **kwargs)

    @property
    def backend_name(self) -> str:
        return "parquet"

    @property
    def cache_dir(self) -> Path | None:
        return self._cache_dir

    @staticmethod
    def _split_by_hylak_id(df: pd.DataFrame) -> dict[int, pd.DataFrame]:
        if df.empty:
            return {}
        return {int(hid): group.drop(columns=["hylak_id"]).reset_index(drop=True) for hid, group in df.groupby("hylak_id")}

    def _register_grid_dependencies(self) -> None:
        for table_name in ("lake_info",):
            self._ensure_view(table_name, root=self._data_dir)
        output_dir = self._config.output_dir
        if output_dir is None:
            return
        for table_name in (
            "quantile_extremes",
            "quantile_abrupt_transitions",
            "pwm_extreme_thresholds",
            "pwm_extreme_labels",
            "pwm_extreme_extremes",
            "eot_results",
            "eot_extremes",
            "comparison_run_status",
        ):
            self._ensure_view(table_name, root=output_dir)

    def _table_path(self, table_name: str) -> Path:
        return self._data_dir / f"{self._config.t.parquet_file(table_name)}.parquet"

    def _read_table_df(self, table_name: str) -> pd.DataFrame:
        table_path = self._table_path(table_name)
        return pd.read_parquet(table_path) if table_path.exists() else pd.DataFrame()
