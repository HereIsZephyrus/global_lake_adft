"""Configuration for lakeviz global grid visualization."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lakesource.provider import LakeProvider


@dataclass(frozen=True)
class VizConfig:
    """Global visualization configuration (fonts, DPI, etc.).

    Attributes:
        font_en: English font family name.
        font_cjk: CJK font family name for Chinese/Japanese/Korean text.
        default_dpi: Default DPI for all figure exports.
    """

    font_en: str = "Times New Roman"
    font_cjk: str = "SimSun"
    default_dpi: int = 300


DEFAULT_VIZ_CONFIG = VizConfig()


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
