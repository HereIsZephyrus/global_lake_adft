"""Shared data types and constants for the monthly transition workflow.

These schemas are owned by the data layer (lakesource) so that both
lakesource and lakeanalysis can reference them without creating a
circular dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class QuantileResult:
    """Workflow outputs for one lake."""

    hylak_id: int | None
    climatology_df: pd.DataFrame
    labels_df: pd.DataFrame
    extremes_df: pd.DataFrame
    transitions_df: pd.DataFrame
    q_low: float
    q_high: float


@dataclass(frozen=True)
class QuantileServiceConfig:
    """Config for one-lake monthly transition execution."""

    min_valid_per_month: int | None = 20
    min_valid_observations: int | None = 240


@dataclass(frozen=True)
class QuantileBatchConfig:
    """Config for DB batch execution and summary output."""

    output_root: Path
    chunk_size: int = 10_000
    limit_id: int | None = None
    min_valid_per_month: int | None = None
    min_valid_observations: int | None = None
    build_summary_cache: bool = True
    plot_summary: bool = True

    @property
    def service_config(self) -> QuantileServiceConfig:
        """Service-level config used for each lake in batch mode."""
        return QuantileServiceConfig(
            min_valid_per_month=self.min_valid_per_month,
            min_valid_observations=self.min_valid_observations,
        )


RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"
