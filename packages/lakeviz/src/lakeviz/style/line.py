"""Line plot style."""

from __future__ import annotations

from dataclasses import dataclass

from .base import DrawStyle


@dataclass(frozen=True)
class LineStyle(DrawStyle):
    marker: str | None = None
    markersize: float = 3
    markerfacecolor: str = "white"
    markeredgewidth: float = 1.5