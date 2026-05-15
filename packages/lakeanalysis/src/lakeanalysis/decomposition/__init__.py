"""Decomposition methods for lake area anomaly index computation.

Each method produces a :class:`DecompositionResult` whose ``index_value``
column is a cross-month comparable anomaly index consumed by downstream
 quantile / PWM extreme calculators.

``MonthlyClimatologyMethod`` remains available for legacy compatibility but is
deprecated. Prefer ``STLPercentileMethod`` for new runs.
"""

from __future__ import annotations

from .base import DecompositionMethod, DecompositionResult
from .monthly_climatology import MonthlyClimatologyMethod
from .stl_percentile import STLPercentileMethod

__all__ = [
    "DecompositionMethod",
    "DecompositionResult",
    "MonthlyClimatologyMethod",
    "STLPercentileMethod",
]
