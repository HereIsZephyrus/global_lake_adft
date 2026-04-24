"""EOT data access layer (read + cache)."""

from .reader import (
    fetch_available_quantiles,
    fetch_eot_results_with_coords,
)

__all__ = [
    "fetch_available_quantiles",
    "fetch_eot_results_with_coords",
]
