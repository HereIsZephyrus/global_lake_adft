"""Structured query spec for building a worker-scoped LakeDataset."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LakeDatasetQuery:
    algorithm: str | None = None
    id_range: tuple[int, int] | None = None
    id_subset: frozenset[int] | None = None
    require_quality: bool = True
    exclude_done: bool = True
    fields: tuple[str, ...] = ("series", "frozen_mask")
