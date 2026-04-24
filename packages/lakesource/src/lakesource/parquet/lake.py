"""Parquet-based lake data read functions (stub).

This module provides the parquet-backend equivalents of the postgres lake
operations.  Each function mirrors the signature of its postgres counterpart
but reads from Parquet files via DuckDB instead of PostgreSQL.

All functions currently raise NotImplementedError and will be implemented in
a future iteration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def fetch_lake_area(
    data_dir: Path | str,
    limit_id: int | None = None,
) -> dict[int, pd.DataFrame]:
    """Fetch all lake_area rows from Parquet and split by hylak_id.

    Args:
        data_dir: Directory containing lake_area.parquet.
        limit_id: If given, only rows with id < limit_id are returned.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_lake_area is not yet implemented for parquet backend")


def fetch_lake_area_chunk(
    data_dir: Path | str,
    chunk_start: int,
    chunk_end: int,
) -> dict[int, pd.DataFrame]:
    """Fetch lake_area rows for a hylak_id range [chunk_start, chunk_end).

    Args:
        data_dir: Directory containing lake_area.parquet.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_lake_area_chunk is not yet implemented for parquet backend")


def fetch_lake_area_by_ids(
    data_dir: Path | str,
    id_list: list[int],
) -> dict[int, pd.DataFrame]:
    """Fetch lake_area rows for the given hylak_id set.

    Args:
        data_dir: Directory containing lake_area.parquet.
        id_list: List of hylak_id values to fetch.

    Returns:
        Dict mapping hylak_id to a DataFrame with columns [year, month, water_area].

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_lake_area_by_ids is not yet implemented for parquet backend")


def fetch_lake_geometry_wkt_by_ids(
    data_dir: Path | str,
    hylak_ids: list[int],
    **kwargs: Any,
) -> pd.DataFrame:
    """Load lake outlines as WKT from Parquet.

    Args:
        data_dir: Directory containing geometry Parquet files.
        hylak_ids: Lake ids to fetch.

    Returns:
        DataFrame with columns ``hylak_id``, ``wkt``.

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_lake_geometry_wkt_by_ids is not yet implemented for parquet backend")


def fetch_eot_extremes_by_id(
    data_dir: Path | str,
    hylak_id: int,
    threshold_quantile: float | None = None,
) -> pd.DataFrame:
    """Fetch EOT extreme rows for one lake from Parquet.

    Args:
        data_dir: Directory containing eot_extremes.parquet.
        hylak_id: Target lake id.
        threshold_quantile: Optional quantile filter.

    Returns:
        DataFrame with extreme event columns.

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_eot_extremes_by_id is not yet implemented for parquet backend")


def fetch_linear_trend_by_ids(
    data_dir: Path | str,
    id_list: list[int],
) -> dict[int, float | None]:
    """Fetch linear trend from Parquet for the given hylak_ids.

    Args:
        data_dir: Directory containing lake_info.parquet.
        id_list: List of hylak_id values to fetch.

    Returns:
        Dict mapping hylak_id to trend in km²/year (float) or None.

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_linear_trend_by_ids is not yet implemented for parquet backend")


def fetch_frozen_year_months_by_ids(
    data_dir: Path | str,
    id_list: list[int],
) -> dict[int, set[int]]:
    """Fetch frozen YYYYMM keys for the given hylak_ids from Parquet.

    Args:
        data_dir: Directory containing anomaly.parquet.
        id_list: List of hylak_id values to fetch.

    Returns:
        Dict mapping hylak_id to a set of YYYYMM integer keys.

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_frozen_year_months_by_ids is not yet implemented for parquet backend")


def fetch_af_nearest_high_topo(
    data_dir: Path | str,
) -> list[dict]:
    """Fetch af_nearest rows with topo_level > 8 from Parquet.

    Args:
        data_dir: Directory containing af_nearest.parquet.

    Returns:
        List of dicts with keys hylak_id, nearest_id, topo_level.

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_af_nearest_high_topo is not yet implemented for parquet backend")


def fetch_atlas_area_chunk(
    data_dir: Path | str,
    chunk_start: int,
    chunk_end: int,
) -> dict[int, float]:
    """Fetch atlas_area for a hylak_id range from Parquet.

    Args:
        data_dir: Directory containing lake_info.parquet.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.

    Returns:
        Dict mapping hylak_id to atlas_area (float).

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_atlas_area_chunk is not yet implemented for parquet backend")


def fetch_seasonal_amplitude_chunk(
    data_dir: Path | str,
    chunk_start: int,
    chunk_end: int,
) -> dict[int, float | None]:
    """Fetch seasonal amplitude for a hylak_id range from Parquet.

    Args:
        data_dir: Directory containing lake_info.parquet.
        chunk_start: Inclusive lower bound of the hylak_id range.
        chunk_end: Exclusive upper bound of the hylak_id range.

    Returns:
        Dict mapping hylak_id to CV (float or None).

    Raises:
        NotImplementedError: Not yet implemented for parquet backend.
    """
    raise NotImplementedError("fetch_seasonal_amplitude_chunk is not yet implemented for parquet backend")
