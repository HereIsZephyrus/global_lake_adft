"""Histogram style."""

from __future__ import annotations

from dataclasses import dataclass

from .base import DrawStyle


@dataclass(frozen=True)
class HistogramStyle(DrawStyle):
    bins: int = 40
    density: bool = False
    edgecolor: str = "white"
    linewidth: float = 0.4