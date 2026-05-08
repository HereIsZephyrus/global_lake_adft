"""Metadata reading ports."""

from __future__ import annotations

from typing import Protocol


class AtlasAreaReadPort(Protocol):
    def fetch_atlas_area_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float]: ...
    def fetch_atlas_area_by_ids(
        self, id_list: list[int]
    ) -> dict[int, float]: ...


class LakeInfoReadPort(Protocol):
    def fetch_seasonal_amplitude_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, float | None]: ...
    def fetch_seasonal_amplitude_by_ids(
        self, id_list: list[int]
    ) -> dict[int, float | None]: ...
    def fetch_linear_trend_by_ids(
        self, id_list: list[int]
    ) -> dict[int, float | None]: ...
    def fetch_max_hylak_id(self) -> int: ...
    def count_source_hylak_ids_in_range(
        self, chunk_start: int, chunk_end: int
    ) -> int: ...
    def fetch_source_hylak_ids_in_range(
        self, chunk_start: int, chunk_end: int
    ) -> set[int]: ...


class FrozenMonthReadPort(Protocol):
    def fetch_frozen_year_months_chunk(
        self, chunk_start: int, chunk_end: int
    ) -> dict[int, set[int]]: ...
    def fetch_frozen_year_months_by_ids(
        self, id_list: list[int]
    ) -> dict[int, set[int]]: ...
