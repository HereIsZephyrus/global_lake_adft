"""Global spatial visualization for lake drought-flood abrupt transitions.

Architecture (three-layer separation + declarative styles):

  style/    — Declarative style system (DrawStyle, AxisStyle, presets, Theme)
  draw/     — Minimal draw primitives (draw_line, draw_scatter, …)
  domain/   — Domain-level draw functions (draw_mrl, draw_pp, …)
  layout.py — Figure composition (create_figure, save)

Each ``draw_*`` function operates on a caller-provided ``ax`` and stamps
``ax._ax_kind`` (AxKind.STATISTICAL or AxKind.GEOGRAPHIC) so that the
layout layer can validate placement.

Backward-compatible ``plot_*`` wrappers are still available in the
domain sub-modules.
"""

from __future__ import annotations

from .config import GlobalGridConfig, VizConfig, DEFAULT_VIZ_CONFIG
from .grid import agg_to_grid_matrix, build_grid_counts, build_grid_stats
from .map_plot import plot_global_grid, draw_global_grid
from .render import setup_web_backend, fig_to_base64
from .layout import create_figure, save
from .style.base import AxKind
from .style import Theme
from .style.presets import NCL_CMAPS

__all__ = [
    "GlobalGridConfig",
    "VizConfig",
    "DEFAULT_VIZ_CONFIG",
    "agg_to_grid_matrix",
    "build_grid_counts",
    "build_grid_stats",
    "plot_global_grid",
    "draw_global_grid",
    "setup_web_backend",
    "fig_to_base64",
    "create_figure",
    "save",
    "AxKind",
    "Theme",
    "NCL_CMAPS",
]