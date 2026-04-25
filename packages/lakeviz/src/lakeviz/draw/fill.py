"""Draw a shaded confidence band on an Axes."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.style.fill import FillStyle
from lakeviz.style.base import AxKind, stamp_ax


def draw_fill_between(
    ax: plt.Axes,
    x: np.ndarray | pd.Series,
    y_lower: np.ndarray | pd.Series,
    y_upper: np.ndarray | pd.Series,
    *,
    style: FillStyle = FillStyle(),
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.fill_between(
        x, y_lower, y_upper,
        facecolor=style.facecolor,
        alpha=style.alpha,
        edgecolor=style.edgecolor,
        label=style.label,
    )