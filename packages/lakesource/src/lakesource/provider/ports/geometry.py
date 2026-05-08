"""Geometry and impact-pair reading port."""

from __future__ import annotations

from typing import Protocol

import pandas as pd


class GeometryReadPort(Protocol):
    def fetch_lake_geometry_wkt_by_ids(
        self,
        hylak_ids: list[int],
        *,
        simplify_tolerance_meters: float | None = None,
    ) -> pd.DataFrame: ...
    def fetch_lake_centroids_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> list[tuple[int, str]]: ...


class ImpactPairReadPort(Protocol):
    def fetch_impact_pairs(self) -> list[dict[str, int]]: ...
    def fetch_af_nearest_high_topo(self) -> list[dict]: ...
