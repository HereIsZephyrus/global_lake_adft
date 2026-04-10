"""Typed configuration for monthly transition workflows."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MonthlyTransitionServiceConfig:
    """Config for one-lake monthly transition execution."""

    min_valid_per_month: int | None = 20
    min_valid_observations: int | None = 240


@dataclass(frozen=True)
class MonthlyTransitionBatchConfig:
    """Config for DB batch execution and summary output."""

    output_root: Path
    chunk_size: int = 10_000
    limit_id: int | None = None
    min_valid_per_month: int | None = None
    min_valid_observations: int | None = None
    build_summary_cache: bool = True
    plot_summary: bool = True

    @property
    def service_config(self) -> MonthlyTransitionServiceConfig:
        """Service-level config used for each lake in batch mode."""
        return MonthlyTransitionServiceConfig(
            min_valid_per_month=self.min_valid_per_month,
            min_valid_observations=self.min_valid_observations,
        )
