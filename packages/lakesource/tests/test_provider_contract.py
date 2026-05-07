"""Tests for lakesource provider contracts and lazy exports."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

import lakesource.postgres as postgres
from lakesource.provider.base import LakeProvider
from lakesource.provider.factory import create_provider
from lakesource.provider.parquet_provider import ParquetLakeProvider
from lakesource.config import Backend, SourceConfig


class FakeGridProvider(LakeProvider):
    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, pd.DataFrame]:
        return {}

    def fetch_lake_area_by_ids(self, id_list: list[int]) -> dict[int, pd.DataFrame]:
        return {}

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int) -> dict[int, set[int]]:
        return {}

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]) -> dict[int, set[int]]:
        return {}

    def fetch_max_hylak_id(self) -> int:
        return 0

    def fetch_lake_geometry_wkt_by_ids(
        self,
        hylak_ids: list[int],
        *,
        simplify_tolerance_meters: float | None = None,
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def fetch_grid_agg(
        self,
        query_name: str,
        resolution: float = 0.5,
        *,
        refresh: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        return pd.DataFrame(
            [{"query_name": query_name, "resolution": resolution, "refresh": refresh, **kwargs}]
        )

    @property
    def backend_name(self) -> str:
        return "fake"

    @property
    def cache_dir(self) -> Path | None:
        return None


def test_provider_default_grid_wrappers_delegate_to_fetch_grid_agg() -> None:
    provider = FakeGridProvider()

    assert provider.fetch_extremes_grid_agg().iloc[0]["query_name"] == "quantile.extremes"
    assert provider.fetch_transitions_by_type_grid_agg().iloc[0]["query_name"] == "quantile.transitions_by_type"
    assert provider.fetch_eot_convergence_grid_agg("high", 0.95).iloc[0]["query_name"] == "eot.convergence"
    assert provider.fetch_pwm_monthly_threshold_grid_agg().iloc[0]["query_name"] == "pwm.monthly_threshold"


def test_create_provider_loads_env_and_constructs_parquet_provider(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PARQUET_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATA_BACKEND", "parquet")

    provider = create_provider()

    assert isinstance(provider, ParquetLakeProvider)


def test_parquet_provider_max_hylak_id_uses_lake_info(tmp_path: Path) -> None:
    pd.DataFrame(
        [
            {"hylak_id": 1, "lake_area": 10.0},
            {"hylak_id": 5, "lake_area": 20.0},
        ]
    ).to_parquet(tmp_path / "lake_info.parquet", index=False)
    pd.DataFrame(
        [
            {"hylak_id": 1, "year_month": pd.Timestamp("2000-01-01"), "water_area": 10.0},
            {"hylak_id": 3, "year_month": pd.Timestamp("2000-02-01"), "water_area": 11.0},
        ]
    ).to_parquet(tmp_path / "lake_area.parquet", index=False)

    provider = ParquetLakeProvider(SourceConfig(backend=Backend.PARQUET, data_dir=tmp_path))

    assert provider.fetch_max_hylak_id() == 5


def test_postgres_lazy_exports_resolve_expected_symbols() -> None:
    assert postgres.fetch_max_lake_info_hylak_id
    assert postgres.fetch_quality_done_hylak_ids_in_range
    assert postgres.fetch_source_hylak_ids_in_range
