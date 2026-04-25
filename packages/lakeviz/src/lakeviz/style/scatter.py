"""Scatter plot style."""

from __future__ import annotations

from dataclasses import dataclass

from .base import DrawStyle


@dataclass(frozen=True)
class ScatterStyle(DrawStyle):
    s: float = 16
    edgecolors: str | None = None
    linewidths: float = 0.8
    marker: str = "o"
    rasterized: bool = False