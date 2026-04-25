"""Minimal draw primitives — each function operates on a single Axes.

Every ``draw_*`` function:
  1. Accepts an ``ax`` (never creates its own Figure).
  2. Accepts a style object from ``lakeviz.style``.
  3. Stamps ``ax._ax_kind`` via ``stamp_ax``.
  4. Returns ``None`` (draws in-place).
"""

from .line import draw_line
from .scatter import draw_scatter
from .bar import draw_bar
from .histogram import draw_histogram
from .fill import draw_fill_between
from .reference import draw_axhline, draw_axvline, draw_diagonal
from .annotate import draw_annotate_point, draw_text_box

__all__ = [
    "draw_line",
    "draw_scatter",
    "draw_bar",
    "draw_histogram",
    "draw_fill_between",
    "draw_axhline",
    "draw_axvline",
    "draw_diagonal",
    "draw_annotate_point",
    "draw_text_box",
]