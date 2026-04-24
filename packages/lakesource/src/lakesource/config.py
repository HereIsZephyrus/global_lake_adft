"""Data source configuration with backend selection."""

from __future__ import annotations

from enum import Enum
from dataclasses import dataclass, field
from pathlib import Path

from .table_config import TableConfig


class Backend(str, Enum):
    """Available data backends."""
    POSTGRES = "postgres"
    PARQUET = "parquet"


@dataclass(frozen=True)
class SourceConfig:
    """Configuration for data access, including backend selection and time filtering.

    Attributes:
        backend: Which data backend to use (postgres or parquet).
        data_dir: Path to parquet files directory (required when backend=parquet).
        workflow_version: Workflow version string for monthly transition data.
        year_start: Optional start year filter (None = no filter).
        year_end: Optional end year filter (None = no filter).
        tables: Table name mapping. None → auto-load from default config.toml.
    """

    backend: Backend = Backend.POSTGRES
    data_dir: Path | None = None
    workflow_version: str = "monthly-transition-v1"
    year_start: int | None = None
    year_end: int | None = None
    tables: TableConfig | None = None

    def __post_init__(self) -> None:
        normalized = self.workflow_version.strip()
        if not normalized:
            raise ValueError("workflow_version must not be empty")
        object.__setattr__(self, "workflow_version", normalized)
        if self.backend == Backend.PARQUET and self.data_dir is None:
            raise ValueError("data_dir is required when backend=parquet")
        if self.tables is None:
            object.__setattr__(self, "tables", TableConfig.default())

    @property
    def t(self) -> TableConfig:
        """Shortcut to the resolved TableConfig (never None after __post_init__)."""
        assert self.tables is not None
        return self.tables