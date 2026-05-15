"""Batch IO abstractions backed by lakesource providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from lakesource.config import Backend, SourceConfig
from lakesource.provider.base import LakeProvider
from lakesource.provider.factory import create_provider

from .task_spec import get_batch_task_spec


class BatchReader(ABC):
    """Batch Reader."""

    @abstractmethod
    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        """Fetch lake area chunk."""

    @abstractmethod
    def fetch_lake_area_by_ids(self, id_list: list[int]):
        """Fetch lake area by ids."""

    @abstractmethod
    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        """Fetch frozen year months chunk."""

    @abstractmethod
    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        """Fetch frozen year months by ids."""

    @abstractmethod
    def fetch_max_hylak_id(self) -> int:
        """Fetch max hylak id."""

    @abstractmethod
    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
        """Fetch done ids."""

    @abstractmethod
    def fetch_quality_ids(self) -> set[int]:
        """Fetch quality ids."""


class BatchWriter(ABC):
    """Batch Writer."""

    @abstractmethod
    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        """Persist."""

    @abstractmethod
    def ensure_schema(self, algorithm: str) -> None:
        """Ensure schema."""

    @abstractmethod
    def truncate_run_status(self, algorithm: str) -> None:
        """Truncate the run_status table for *algorithm* after a full run.

        This clears incremental progress so the next run starts fresh.
        Only called when running without a lake filter (full run).
        """


class ProviderBatchReader(BatchReader):
    """Provider Batch Reader."""

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
        self._parquet_output_dir = config.output_dir if config.backend == Backend.PARQUET else None

    def fetch_lake_area_chunk(self, chunk_start: int, chunk_end: int):
        """Fetch lake area chunk."""
        return self._provider.fetch_lake_area_chunk(chunk_start, chunk_end)

    def fetch_lake_area_by_ids(self, id_list: list[int]):
        """Fetch lake area by ids."""
        return self._provider.fetch_lake_area_by_ids(id_list)

    def fetch_frozen_year_months_chunk(self, chunk_start: int, chunk_end: int):
        """Fetch frozen year months chunk."""
        return self._provider.fetch_frozen_year_months_chunk(chunk_start, chunk_end)

    def fetch_frozen_year_months_by_ids(self, id_list: list[int]):
        """Fetch frozen year months by ids."""
        return self._provider.fetch_frozen_year_months_by_ids(id_list)

    def fetch_max_hylak_id(self) -> int:
        """Fetch max hylak id."""
        return self._provider.fetch_max_hylak_id()

    def fetch_done_ids(self, algorithm: str, chunk_start: int, chunk_end: int) -> set[int]:
        """Fetch done ids."""
        spec = get_batch_task_spec(algorithm)
        table_name = self._done_table if self._done_table is not None else spec.done_table
        if table_name is None:
            return set()
        done_requires_status = self._done_requires_status or spec.done_requires_status
        status = "done" if done_requires_status else None
        if self._parquet_output_dir is not None:
            return _read_done_ids_from_parquet_output(
                self._parquet_output_dir,
                table_name,
                chunk_start,
                chunk_end,
                status=status,
            )
        return self._provider.fetch_done_ids(table_name, chunk_start, chunk_end, status=status)

    def fetch_quality_ids(self) -> set[int]:
        """Fetch quality ids."""
        rows = self._provider.fetch_rows("area_quality", 0, 2 ** 31)
        return {
            int(row["hylak_id"])
            for row in rows
            if row.get("hylak_id") is not None
        }


class ProviderBatchWriter(BatchWriter):
    """Provider Batch Writer."""

    _RUN_STATUS_TABLES: dict[str, str] = {
        "quantile": "quantile_run_status",
        "pwm_extreme": "pwm_extreme_run_status",
        "pwm_hawkes": "pwm_hawkes_run_status",
        "eot_hawkes": "eot_hawkes_run_status",
        "eot": "eot_run_status",
        "comparison": "comparison_run_status",
        "hawkes_comparison": "hawkes_comparison",
        "area_quality": "quality_run_status",
    }

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
        self._output_dir = config.output_dir if config.backend == Backend.PARQUET else None

    def persist(self, rows_by_table: dict[str, list[dict]]) -> None:
        """Persist."""
        if self._output_dir is not None:
            _persist_rows_to_parquet_output(self._output_dir, rows_by_table)
            return
        for table_name, rows in rows_by_table.items():
            if rows:
                self._provider.upsert_rows(table_name, rows)

    def ensure_schema(self, algorithm: str) -> None:
        """Ensure schema."""
        spec = get_batch_task_spec(algorithm)
        ensure_tables = self._ensure_tables or list(spec.ensure_tables)
        if self._output_dir is not None:
            self._output_dir.mkdir(parents=True, exist_ok=True)
            for table_name in ensure_tables:
                _ensure_parquet_output_table(self._output_dir, table_name)
            return
        for table_name in ensure_tables:
            self._provider.ensure_table(table_name)

    def truncate_run_status(self, algorithm: str) -> None:
        """Truncate run status."""
        table = self._RUN_STATUS_TABLES.get(algorithm)
        if table:
            if self._output_dir is not None:
                _table_output_path(self._output_dir, table).unlink(missing_ok=True)
                return
            self._provider.truncate_table(table)


def _table_output_path(output_dir: Path, table_name: str) -> Path:
    """Resolve the parquet output file path for a result table."""
    return output_dir / f"{table_name}.parquet"


def _ensure_parquet_output_table(output_dir: Path, table_name: str) -> None:
    """Ensure the output directory exists for a result table."""
    del table_name
    output_dir.mkdir(parents=True, exist_ok=True)


def _persist_rows_to_parquet_output(output_dir: Path, rows_by_table: dict[str, list[dict]]) -> None:
    """Upsert result rows into output parquet tables."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for table_name, rows in rows_by_table.items():
        if not rows:
            continue
        table_path = _table_output_path(output_dir, table_name)
        new_df = pd.DataFrame(rows)
        if table_path.exists():
            existing_df = pd.read_parquet(table_path)
        else:
            existing_df = pd.DataFrame()
        if existing_df.empty:
            merged = new_df
        elif "hylak_id" in new_df.columns:
            combined = pd.concat([existing_df, new_df], ignore_index=True)
            merged = combined.drop_duplicates(subset=["hylak_id"], keep="last").reset_index(drop=True)
        else:
            merged = pd.concat([existing_df, new_df], ignore_index=True)
        merged.to_parquet(table_path, index=False)


def _read_done_ids_from_parquet_output(
    output_dir: Path,
    table_name: str,
    chunk_start: int,
    chunk_end: int,
    *,
    status: str | None,
) -> set[int]:
    """Read done ids from a parquet result table stored under output_dir."""
    table_path = _table_output_path(output_dir, table_name)
    if not table_path.exists():
        return set()
    df = pd.read_parquet(table_path)
    if df.empty or "hylak_id" not in df.columns:
        return set()
    mask = (df["hylak_id"] >= chunk_start) & (df["hylak_id"] < chunk_end)
    if status is not None and "status" in df.columns:
        mask &= df["status"] == status
    return set(df.loc[mask, "hylak_id"].astype(int).tolist())


def build_batch_reader(config: SourceConfig | None = None) -> BatchReader:
    """Build batch reader."""
    return build_provider_batch_reader(config)


def build_provider_batch_reader(
    config: SourceConfig | None = None,
    *,
    done_table: str | None = None,
    done_requires_status: bool = False,
) -> BatchReader:
    """Build provider batch reader."""
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
    """Build batch writer."""
    return build_provider_batch_writer(config)


def build_provider_batch_writer(
    config: SourceConfig | None = None,
    *,
    ensure_tables: list[str] | None = None,
) -> BatchWriter:
    """Build provider batch writer."""
    config = config or SourceConfig()
    if config.backend not in {Backend.POSTGRES, Backend.PARQUET}:
        raise ValueError(f"Unsupported backend for batch writer: {config.backend!r}")
    return ProviderBatchWriter(create_provider(config), config, ensure_tables=ensure_tables)
