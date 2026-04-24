"""EOT data access layer (SQL-side aggregation + cache)."""

from .reader import (
    fetch_available_quantiles,
    fetch_eot_convergence_grid_agg,
    fetch_eot_converged_grid_agg,
)

__all__ = [
    "fetch_available_quantiles",
    "fetch_eot_convergence_grid_agg",
    "fetch_eot_converged_grid_agg",
]
