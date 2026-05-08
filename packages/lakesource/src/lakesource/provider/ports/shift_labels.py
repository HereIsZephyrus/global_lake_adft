"""Shift labels write port."""

from __future__ import annotations

from typing import Any, Protocol


class ShiftLabelsWritePort(Protocol):
    def ensure_area_shift_labels_table(self) -> None: ...
    def truncate_area_shift_labels(self) -> None: ...
    def upsert_area_shift_labels(
        self, rows: list[dict[str, Any]]
    ) -> None: ...
    def fetch_shift_labels_in_range(
        self, chunk_start: int, chunk_end: int
    ) -> list[dict[str, Any]]: ...
