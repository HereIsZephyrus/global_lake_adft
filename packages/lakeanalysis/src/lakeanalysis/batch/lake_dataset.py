"""Dense in-memory dataset for a worker's lake slice."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


@dataclass(frozen=True)
class LakeDataset:
    hylak_ids: np.ndarray
    year_months: np.ndarray
    values: np.ndarray
    frozen_mask: np.ndarray | None = None
    extra: dict[str, np.ndarray] | None = None

    def __post_init__(self) -> None:
        if self.hylak_ids.ndim != 1:
            raise ValueError("hylak_ids must be 1D")
        if self.year_months.ndim != 1:
            raise ValueError("year_months must be 1D")
        if self.values.ndim != 2:
            raise ValueError("values must be 2D")
        if self.values.shape != (len(self.hylak_ids), len(self.year_months)):
            raise ValueError("values shape must match (n_lakes, n_months)")
        if self.frozen_mask is not None and self.frozen_mask.shape != self.values.shape:
            raise ValueError("frozen_mask shape must match values")
        if self.extra is not None:
            for key, value in self.extra.items():
                if value.shape[0] != len(self.hylak_ids):
                    raise ValueError(f"extra field {key!r} must align with hylak_ids")

    def __len__(self) -> int:
        return int(self.hylak_ids.shape[0])

    def slice(self, start: int, end: int) -> "LakeDataset":
        return self.take(np.arange(start, end, dtype=int))

    def take(self, indices: np.ndarray | list[int]) -> "LakeDataset":
        idx = np.asarray(indices, dtype=int)
        extra = None
        if self.extra is not None:
            extra = {key: value[idx] for key, value in self.extra.items()}
        frozen_mask = None if self.frozen_mask is None else self.frozen_mask[idx]
        return LakeDataset(
            hylak_ids=self.hylak_ids[idx],
            year_months=self.year_months,
            values=self.values[idx],
            frozen_mask=frozen_mask,
            extra=extra,
        )
