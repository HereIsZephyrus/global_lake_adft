"""Shared data types and constants for the monthly anomaly transition workflow."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from lakeanalysis.extreme.models import ExtremeResult, QuantileDiagnostics


@dataclass(frozen=True)
class QuantileResult:
    """Workflow outputs for one lake.

    ``extreme`` holds the shared extreme-event result (labels, events, transitions).
    ``diagnostics`` holds quantile-specific threshold values.
    """

    extreme: ExtremeResult
    diagnostics: QuantileDiagnostics

    @property
    def hylak_id(self) -> int | None:
        return self.extreme.hylak_id

    @property
    def labels_df(self) -> pd.DataFrame:
        return self.extreme.labels_df

    @property
    def extremes_df(self) -> pd.DataFrame:
        return self.extreme.extremes_df

    @property
    def transitions_df(self) -> pd.DataFrame:
        return self.extreme.transitions_df

    @property
    def q_low(self) -> float:
        return self.diagnostics.q_low

    @property
    def q_high(self) -> float:
        return self.diagnostics.q_high


@dataclass(frozen=True)
class QuantileServiceConfig:
    """Config for one-lake monthly transition execution."""

    min_valid_per_month: int | None = 20
    min_valid_observations: int | None = 240
    method: str = "stl"


@dataclass(frozen=True)
class QuantileBatchConfig:
    """Config for DB batch execution and summary output."""

    output_root: Path
    chunk_size: int = 10_000
    limit_id: int | None = None
    min_valid_per_month: int | None = None
    min_valid_observations: int | None = None
    method: str = "stl"
    build_summary_cache: bool = True
    plot_summary: bool = True

    @property
    def service_config(self) -> QuantileServiceConfig:
        """Service-level config used for each lake in batch mode."""
        return QuantileServiceConfig(
            min_valid_per_month=self.min_valid_per_month,
            min_valid_observations=self.min_valid_observations,
            method=self.method,
        )


RUN_STATUS_DONE = "done"
RUN_STATUS_ERROR = "error"
