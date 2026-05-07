"""Batch IO abstractions backed by lakesource providers."""

from __future__ import annotations

from abc import ABC, abstractmethod

from lakesource.config import Backend, SourceConfig
from lakesource.provider.base import LakeProvider
from lakesource.provider.factory import create_provider

from .task_spec import get_batch_task_spec


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


class ProviderBatchReader(BatchReader):
    def __init__(
        self,
        provider: LakeProvider,
        config: SourceConfig,
        *,
        done_table: str | None = None,
        done_requires_status: bool = False,
    ) -> None:
        self._provider = provider
        self._config = config
        self._done_table = done_table
        self._done_requires_status = done_requires_status

    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        return self._provider.fetch_lake_area_chunk(chunk_start, chunk_end)

    def fetch_lake_area_by_ids(self, id_list: list[int]):
        return self._provider.fetch_lake_area_by_ids(id_list)

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        return self._provider.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        return self._provider.fetch_frozen_year_months_by_ids(id_list)

    def fetch_max_hylak_id(self) -> int:
        return self._provider.fetch_max_hylak_id()

    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
        spec = get_batch_task_spec(algorithm)
        table_name = self._done_table if self._done_table is not None else spec.done_table
        if table_name is None:
            return set()
        done_ids = self._provider.fetch_done_ids(table_name, chunk_start, chunk_end)
        done_requires_status = self._done_requires_status or spec.done_requires_status
        if done_requires_status:
            return _filter_done_status_ids(self._provider, table_name, chunk_start, chunk_end, done_ids)
        return done_ids


class ProviderBatchWriter(BatchWriter):
    def __init__(
        self,
        provider: LakeProvider,
        config: SourceConfig,
        *,
        ensure_tables: list[str] | None = None,
    ) -> None:
        self._provider = provider
        self._config = config
        self._ensure_tables = ensure_tables or []

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        for table_name, rows in rows_by_table.items():
            if rows:
                self._provider.upsert_rows(table_name, rows)

    def ensure_schema(self, algorithm: str) -> None:
        spec = get_batch_task_spec(algorithm)
        ensure_tables = self._ensure_tables or list(spec.ensure_tables)
        for table_name in ensure_tables:
            self._provider.ensure_table(table_name)


def _filter_done_status_ids(
    provider: LakeProvider,
    table_name: str,
    chunk_start: int,
    chunk_end: int,
    candidate_ids: set[int],
) -> set[int]:
    if not candidate_ids:
        return set()
    if provider.backend_name == "parquet":
        import pandas as pd

        table_path = provider._data_dir / f"{table_name}.parquet"  # type: ignore[attr-defined]
        if not table_path.exists():
            return set()
        df = pd.read_parquet(table_path)
        if df.empty or not {"hylak_id", "status"}.issubset(df.columns):
            return set(candidate_ids)
        done_df = df[
            (df["hylak_id"] >= chunk_start)
            & (df["hylak_id"] < chunk_end)
            & (df["status"] == "done")
        ]
        return set(done_df["hylak_id"].astype(int).tolist())
    try:
        rows = provider.fetch_rows(table_name, chunk_start, chunk_end)  # type: ignore[attr-defined]
    except AttributeError:
        return set(candidate_ids)
    return {
        int(row["hylak_id"])
        for row in rows
        if row.get("status") == "done" and int(row["hylak_id"]) in candidate_ids
    }


def build_batch_reader(config: SourceConfig | None = None) -> BatchReader:
    return build_provider_batch_reader(config)


def build_provider_batch_reader(
    config: SourceConfig | None = None,
    *,
    done_table: str | None = None,
    done_requires_status: bool = False,
) -> BatchReader:
    config = config or SourceConfig()
    if config.backend not in {Backend.POSTGRES, Backend.PARQUET}:
        raise ValueError(f"Unsupported backend for batch reader: {config.backend!r}")
    return ProviderBatchReader(
        create_provider(config),
        config,
        done_table=done_table,
        done_requires_status=done_requires_status,
    )


def build_batch_writer(config: SourceConfig | None = None) -> BatchWriter:
    return build_provider_batch_writer(config)


def build_provider_batch_writer(
    config: SourceConfig | None = None,
    *,
    ensure_tables: list[str] | None = None,
) -> BatchWriter:
    config = config or SourceConfig()
    if config.backend not in {Backend.POSTGRES, Backend.PARQUET}:
        raise ValueError(f"Unsupported backend for batch writer: {config.backend!r}")
    return ProviderBatchWriter(create_provider(config), config, ensure_tables=ensure_tables)
