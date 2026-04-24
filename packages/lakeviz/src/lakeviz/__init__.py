"""Global spatial visualization for lake drought-flood abrupt transitions.

Subpackages provide domain-specific plot functions that accept only
generic data types (pd.DataFrame, np.ndarray, int, float, str).
"""

from __future__ import annotations

from .config import GlobalGridConfig
from .grid import build_grid_counts
from .map_plot import plot_global_grid
from .query import load_extremes_grid_data, load_transitions_grid_data
from .render import setup_web_backend, fig_to_base64

__all__ = [
    "GlobalGridConfig",
    "build_grid_counts",
    "plot_global_grid",
    "load_extremes_grid_data",
    "load_transitions_grid_data",
    "setup_web_backend",
    "fig_to_base64",
]