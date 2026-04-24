"""Global spatial visualization for lake drought-flood abrupt transitions.

Subpackages provide domain-specific plot functions that accept only
generic data types (pd.DataFrame, np.ndarray, int, float, str).
"""

from __future__ import annotations

from .config import GlobalGridConfig
from .grid import agg_to_grid_matrix, build_grid_counts, build_grid_stats
from .map_plot import plot_global_grid
from .render import setup_web_backend, fig_to_base64

__all__ = [
    "GlobalGridConfig",
    "agg_to_grid_matrix",
    "build_grid_counts",
    "build_grid_stats",
    "plot_global_grid",
    "setup_web_backend",
    "fig_to_base64",
]
