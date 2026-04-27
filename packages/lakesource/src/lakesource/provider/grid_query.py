"""Grid aggregation query registry: inject specific queries into Provider.

Each algorithm package registers its grid aggregation queries via
``register_grid_query()``.  The ``LakeProvider`` ABC exposes a single
``fetch_grid_agg(query_name, ...)`` entry point that dispatches to
the registered query for the current backend.

This replaces the old pattern of adding a new ``@abstractmethod`` to
the ABC for every aggregation variant.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import pandas as pd

log = logging.getLogger(__name__)


@runtime_checkable
class GridAggQuery(Protocol):
    """Protocol for a registered grid aggregation query.

    Each query must provide:
      - ``name``: unique identifier (e.g. "quantile.extremes")
      - ``fetch_parquet``: DuckDB-based implementation
      - ``fetch_postgres``: PostgreSQL-based implementation (optional)
    """

    name: str

    def fetch_parquet(
        self,
        client: Any,
        cache_dir: Path,
        resolution: float,
        *,
        refresh: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame: ...

    def fetch_postgres(
        self,
        config: Any,
        resolution: float,
        *,
        refresh: bool = False,
        **kwargs: Any,
    ) -> pd.DataFrame: ...


_REGISTRY: dict[str, GridAggQuery] = {}


def register_grid_query(query: GridAggQuery) -> None:
    if query.name in _REGISTRY:
        raise ValueError(f"Duplicate grid query: {query.name!r}")
    _REGISTRY[query.name] = query


def get_grid_query(name: str) -> GridAggQuery:
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown grid query: {name!r}. Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def list_grid_queries() -> list[str]:
    return sorted(_REGISTRY.keys())