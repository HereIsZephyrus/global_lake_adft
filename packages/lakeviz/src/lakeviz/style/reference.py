"""Reference line (hline / vline / diagonal) style."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ReferenceLineStyle:
    color: str = "grey"
    linestyle: str = "--"
    linewidth: float = 1.0
    alpha: float = 1.0
    label: str | None = None