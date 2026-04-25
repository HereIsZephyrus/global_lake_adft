"""Configuration for lakeviz global grid visualization."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lakesource.provider import LakeProvider


@dataclass(frozen=True)
class GlobalGridConfig:
    """Configuration for 0.5-degree global grid visualization.

    Attributes:
        provider: Data access strategy (Postgres or Parquet backend).
        resolution: Grid cell size in degrees.
        output_dir: Directory for output figures.
    """

    provider: LakeProvider
    resolution: float = 0.5
    output_dir: Path = Path("figures")
