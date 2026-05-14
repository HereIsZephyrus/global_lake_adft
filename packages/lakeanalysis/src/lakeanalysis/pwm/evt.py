"""PWM method-level EVT route adapters.

This module dispatches between PWM EVT Route A / Route B while keeping the
shared EVT math in ``lakeanalysis.extreme.evt``.
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

from .evt_amplitude import compute_evt_amplitude_strengths
from .evt_index import compute_evt_index_strengths


def compute_pwm_evt_strengths(
    labeled_df: pd.DataFrame,
    *,
    evt_route: Literal["A", "B"] = "A",
    amplitude_column: str = "stl_residual",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Dispatch PWM EVT Route A / B for a labelled PWM result."""
    if evt_route == "A":
        return compute_evt_index_strengths(labeled_df)
    if evt_route == "B":
        return compute_evt_amplitude_strengths(
            labeled_df,
            amplitude_column=amplitude_column,
        )
    raise ValueError(f"Unknown evt_route: {evt_route!r}")
