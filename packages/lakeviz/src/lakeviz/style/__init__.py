"""Declarative style system for lakeviz.

Style classes are frozen dataclasses that can be composed and inherited:
  DrawStyle → LineStyle, ScatterStyle, BarStyle, HistogramStyle, ...
  AxisStyle → per-domain axis presets
  PanelStyle → figure-level layout presets

All domain-specific color/line presets live in ``presets.py``.
"""

from .base import DrawStyle, AxisStyle, PanelStyle, AxKind
from .line import LineStyle
from .scatter import ScatterStyle
from .bar import BarStyle
from .histogram import HistogramStyle
from .fill import FillStyle
from .reference import ReferenceLineStyle
from .presets import Theme

__all__ = [
    "DrawStyle",
    "LineStyle",
    "ScatterStyle",
    "BarStyle",
    "HistogramStyle",
    "FillStyle",
    "ReferenceLineStyle",
    "AxisStyle",
    "PanelStyle",
    "AxKind",
    "Theme",
]