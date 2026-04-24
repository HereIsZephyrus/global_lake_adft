"""EOT visualization: single-lake diagnostics and global distribution maps."""

from .global_map import (
    plot_eot_convergence_map,
    plot_eot_extremes_frequency_map,
    plot_eot_sigma_map,
    plot_eot_threshold_map,
    plot_eot_xi_map,
)
from .plot import (
    plot_eot_extremes_from_db,
    plot_extremes_timeline,
    plot_extremes_with_hawkes,
    plot_location_model,
    plot_mrl,
    plot_parameter_stability,
    plot_pp,
    plot_qq,
    plot_return_levels,
)

__all__ = [
    "plot_eot_convergence_map",
    "plot_eot_extremes_frequency_map",
    "plot_eot_sigma_map",
    "plot_eot_threshold_map",
    "plot_eot_xi_map",
    "plot_mrl",
    "plot_parameter_stability",
    "plot_extremes_timeline",
    "plot_pp",
    "plot_qq",
    "plot_return_levels",
    "plot_location_model",
    "plot_eot_extremes_from_db",
    "plot_extremes_with_hawkes",
]
