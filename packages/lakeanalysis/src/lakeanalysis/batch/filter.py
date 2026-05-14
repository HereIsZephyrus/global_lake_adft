"""Concrete lake filter implementations for batch processing."""

from __future__ import annotations

from typing import Iterable

from .domain import LakeFilter


class RangeFilter(LakeFilter):
    def __init__(self, start: int = 0, end: int | None = None) -> None:
        self.start = start
        self.end = end

    def __call__(self, hylak_ids: Iterable[int]) -> set[int]:
        ids = set(hylak_ids)
        ids = {i for i in ids if i >= self.start}
        if self.end is not None:
            ids = {i for i in ids if i < self.end}
        return ids


class IdSetFilter(LakeFilter):
    def __init__(self, ids: set[int] | list[int]) -> None:
        self._ids = set(ids)

    def __call__(self, hylak_ids: Iterable[int]) -> set[int]:
        return self._ids & set(hylak_ids)

    @property
    def ids(self) -> set[int]:
        return self._ids
