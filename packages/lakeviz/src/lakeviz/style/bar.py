"""Bar chart style."""

from __future__ import annotations

from dataclasses import dataclass

from .base import DrawStyle


@dataclass(frozen=True)
class BarStyle(DrawStyle):
    width: float = 0.8
    edgecolor: str = "white"
    linewidth: float = 0.4