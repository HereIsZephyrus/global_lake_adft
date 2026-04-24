"""Configuration for lakeviz global grid visualization."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lakesource.config import SourceConfig


@dataclass(frozen=True)
class GlobalGridConfig:
    """Configuration for 0.5-degree global grid visualization.

    Attributes:
        source: Data source configuration (backend, time filters, etc.).
        resolution: Grid cell size in degrees.
        output_dir: Directory for output figures.
    """

    source: SourceConfig
    resolution: float = 0.5
    output_dir: Path = Path("figures")
