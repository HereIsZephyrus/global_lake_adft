"""Draw a scatter series on an Axes."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.style.scatter import ScatterStyle
from lakeviz.style.base import AxKind, stamp_ax


def draw_scatter(
    ax: plt.Axes,
    x: np.ndarray | pd.Series,
    y: np.ndarray | pd.Series,
    *,
    style: ScatterStyle = ScatterStyle(),
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.scatter(
        x, y,
        color=style.color,
        s=style.s,
        alpha=style.alpha,
        marker=style.marker,
        edgecolors=style.edgecolors,
        linewidths=style.linewidths,
        label=style.label,
        zorder=style.zorder,
        rasterized=style.rasterized,
    )