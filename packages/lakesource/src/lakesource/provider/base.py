"""LakeProvider ABC: backend-oriented lake data access contract."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

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
