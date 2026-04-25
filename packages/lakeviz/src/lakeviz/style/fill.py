"""Fill-between (confidence band) style."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillStyle:
    facecolor: str = "steelblue"
    alpha: float = 0.2
    edgecolor: str | None = None
    label: str | None = None