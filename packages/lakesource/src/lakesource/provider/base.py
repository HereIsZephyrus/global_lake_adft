"""LakeProvider ABC: backend-oriented lake data access contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pandas as pd


class LakeProvider(ABC):
    """Strategy interface for lake data access.

    Implementations:
        - PostgresLakeProvider: psycopg-based, full read/write
        - ParquetLakeProvider: DuckDB-based, read-only

    The interface covers shared backend reads plus visualization/export access.
    Batch-specific workflow semantics live in lakeanalysis.batch.io.
    """

    # ------------------------------------------------------------------
    # Core reads (shared by all algorithms)
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_lake_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, pd.DataFrame]:
        ...

    @abstractmethod
    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
        ...

    @abstractmethod
    def fetch_frozen_year_months_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, set[int]]:
        ...

    @abstractmethod
    def fetch_frozen_year_months_by_ids(
        self, id_list: list[int]
    ) -> dict[int, set[int]]:
        ...

    @abstractmethod
    def fetch_max_hylak_id(self) -> int:
        ...

    def fetch_done_ids(self, table_name: str, chunk_start: int, chunk_end: int) -> set[int]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support done-id reads")

    def fetch_atlas_area_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, float]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support atlas-area reads")

    def fetch_atlas_area_by_ids(self, id_list: list[int]) -> dict[int, float]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support atlas-area reads")

    def fetch_seasonal_amplitude_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float | None]:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support seasonal-amplitude reads"
        )

    def fetch_seasonal_amplitude_by_ids(self, id_list: list[int]) -> dict[int, float | None]:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support seasonal-amplitude reads"
        )

    def ensure_table(self, table_name: str) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not support schema writes")

    def truncate_table(self, table_name: str) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not support truncation")

    def upsert_rows(self, table_name: str, rows: list[dict[str, Any]]) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not support writes")

    def fetch_rows(self, table_name: str, chunk_start: int, chunk_end: int) -> list[dict[str, Any]]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support row fetches")

    def delete_ids(self, table_name: str, hylak_ids: list[int]) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not support deletes")

    def fetch_area_statuses(self) -> dict[int, tuple[str, int]]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support area-status reads")

    def fetch_zero_quantile_flags(self) -> dict[int, int]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support zero-flag reads")

    def clear_zero_quantile_flag(self, hylak_ids: list[int]) -> int:
        raise NotImplementedError(f"{self.__class__.__name__} does not support zero-flag writes")

    def find_nonzero_quantile_lakes(self, hylak_ids: list[int], quantile: float) -> set[int]:
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support zero-quantile recheck"
        )

    def update_area_anomaly_flags(self, updates: list[tuple[int, int]]) -> None:
        raise NotImplementedError(f"{self.__class__.__name__} does not support anomaly-flag updates")

    def fetch_impact_pairs(self) -> list[dict[str, int]]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support impact-pair reads")

    def fetch_lake_centroids_chunk(self, chunk_start: int, chunk_end: int) -> list[tuple[int, str]]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support centroid reads")

    def lookup_pfaf_chunk(self, centroids: list[tuple[int, str]]) -> dict[int, int | None]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support pfaf lookup")

    def fetch_type1_lake_records(self) -> list[dict[str, int | float | None]]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support type1-lake reads")

    def fetch_non_type1_lake_records(
        self, limit_id: int | None = None
    ) -> list[dict[str, int | float | None]]:
        raise NotImplementedError(f"{self.__class__.__name__} does not support non-type1-lake reads")

    @abstractmethod
    def fetch_lake_geometry_wkt_by_ids(
        self,
        hylak_ids: list[int],
        *,
        simplify_tolerance_meters: float | None = None,
    ) -> pd.DataFrame:
        ...

    # ------------------------------------------------------------------
    # Aggregation reads (lakeviz global maps)
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_grid_agg(
        self,
        query_name: str,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch a named grid aggregation query.

        query_name identifies the registered aggregation (e.g. "quantile.extremes",
        "eot.convergence", "pwm.exceedance").  kwargs are query-specific parameters.
        """
        ...

    def fetch_extremes_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("quantile.extremes", resolution, refresh=refresh)

    def fetch_extremes_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("quantile.extremes_by_type", resolution, refresh=refresh)

    def fetch_transitions_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("quantile.transitions", resolution, refresh=refresh)

    def fetch_transitions_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("quantile.transitions_by_type", resolution, refresh=refresh)

    def fetch_eot_convergence_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        return self.fetch_grid_agg(
            "eot.convergence",
            resolution,
            refresh=refresh,
            tail=tail,
            threshold_quantile=threshold_quantile,
        )

    def fetch_eot_converged_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        return self.fetch_grid_agg(
            "eot.converged",
            resolution,
            refresh=refresh,
            tail=tail,
            threshold_quantile=threshold_quantile,
        )

    def fetch_pwm_convergence_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("pwm.convergence", resolution, refresh=refresh)

    def fetch_pwm_converged_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("pwm.converged", resolution, refresh=refresh)

    def fetch_pwm_monthly_threshold_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        return self.fetch_grid_agg("pwm.monthly_threshold", resolution, refresh=refresh)

    def fetch_pwm_exceedance_grid_agg(
        self, resolution: float = 0.5, *, p_high: float = 0.05, p_low: float = 0.05,
        refresh: bool = False,
    ) -> pd.DataFrame:
        return self.fetch_grid_agg(
            "pwm.exceedance",
            resolution,
            refresh=refresh,
            p_high=p_high,
            p_low=p_low,
        )

    def fetch_pwm_monthly_exceedance_grid_agg(
        self, resolution: float = 0.5, *, p_high: float = 0.05, p_low: float = 0.05,
        refresh: bool = False,
    ) -> pd.DataFrame:
        return self.fetch_grid_agg(
            "pwm.monthly_exceedance",
            resolution,
            refresh=refresh,
            p_high=p_high,
            p_low=p_low,
        )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def backend_name(self) -> str:
        ...

    @property
    @abstractmethod
    def cache_dir(self) -> Path | None:
        ...
