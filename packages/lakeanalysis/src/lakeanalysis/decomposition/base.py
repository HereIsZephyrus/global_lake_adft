"""Base types for decomposition methods."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import pandas as pd


@dataclass
class DecompositionResult:
    """Output of any decomposition method.

    ``index_df`` must contain columns:

    * ``year``, ``month``, ``water_area`` — original observation data.
    * ``year_month_key``, ``month_ordinal`` — time-ordering helpers.
    * ``index_value`` — cross-month comparable anomaly index (float).
    """

    index_df: pd.DataFrame
    metadata: dict

    @property
    def method_name(self) -> str:
        """Short name of the decomposition method used."""
        return self.metadata.get("method", "unknown")


@runtime_checkable
class DecompositionMethod(Protocol):
    """Protocol that every decomposition method must satisfy."""

    def decompose(self, series_df: pd.DataFrame) -> DecompositionResult:
        """Compute the anomaly index for a single lake."""
        ...

    @property
    def method_name(self) -> str:
        """Short name of this method."""
        ...
