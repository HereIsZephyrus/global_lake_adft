"""LakeProvider ABC: unified read/write interface for lake data access."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class LakeProvider(ABC):
    """Strategy interface for lake data access.

    Implementations:
        - PostgresLakeProvider: psycopg-based, full read/write
        - ParquetLakeProvider: DuckDB-based, read-only

    The interface covers three consumer groups:
        1. lakeanalysis batch: core reads + writes + schema management
        2. lakeviz global maps: aggregation reads (grid maps)
        3. export scripts: core reads for dumping to parquet
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
    # Algorithm-specific reads (done-id checking)
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_done_ids(
        self, algorithm: str, chunk_start: int, chunk_end: int
    ) -> set[int]:
        ...

    @abstractmethod
    def count_done_ids(
        self, algorithm: str, chunk_start: int, chunk_end: int
    ) -> int:
        ...

    # ------------------------------------------------------------------
    # Aggregation reads (lakeviz global maps)
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_extremes_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_extremes_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_transitions_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_transitions_by_type_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_eot_convergence_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_eot_converged_grid_agg(
        self,
        tail: str,
        threshold_quantile: float,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_pwm_convergence_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def fetch_pwm_converged_grid_agg(
        self, resolution: float = 0.5, *, refresh: bool = False
    ) -> pd.DataFrame:
        ...

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    @abstractmethod
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        ...

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    @abstractmethod
    def ensure_schema(self, algorithm: str) -> None:
        ...

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
