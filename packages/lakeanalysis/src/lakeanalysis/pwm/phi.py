"""Utilities for mapping EVT strength to positive S_k weights."""

from __future__ import annotations

import numpy as np
import pandas as pd


def map_strength_to_phi(
    strengths: np.ndarray,
    *,
    method: str = "identity",
    reference: float | None = None,
) -> np.ndarray:
    """Map non-negative event strengths to non-negative ``phi`` weights.

    Args:
        strengths: Event strength array.
        method: Mapping method. One of ``identity``, ``log1p``, ``normalize``.
        reference: Positive reference scale used by ``normalize``. If omitted,
            the mean positive strength is used.

    Returns:
        Array of non-negative ``phi`` values with the same shape as ``strengths``.
    """
    values = np.asarray(strengths, dtype=float)
    if np.any(~np.isfinite(values)):
        raise ValueError("strengths must be finite")
    if np.any(values < 0.0):
        raise ValueError("strengths must be non-negative")

    if method == "identity":
        return values.copy()
    if method == "log1p":
        return np.log1p(values)
    if method == "normalize":
        ref = reference
        if ref is None:
            positive = values[values > 0.0]
            ref = float(np.mean(positive)) if len(positive) > 0 else 1.0
        if ref <= 0.0:
            raise ValueError("reference must be positive for normalize")
        return values / float(ref)
    raise ValueError(f"Unknown phi mapping method: {method!r}")


def map_strength_df_to_phi(
    strengths_df: pd.DataFrame,
    *,
    method: str = "identity",
    reference: float | None = None,
) -> pd.DataFrame:
    """Attach a ``phi`` column to a month-level strength table."""
    required_cols = {"year", "month", "event_strength"}
    missing = required_cols - set(strengths_df.columns)
    if missing:
        raise ValueError(f"strengths_df missing required columns: {sorted(missing)}")

    result = strengths_df.copy()
    result["phi"] = map_strength_to_phi(
        result["event_strength"].to_numpy(dtype=float),
        method=method,
        reference=reference,
    )
    return result
