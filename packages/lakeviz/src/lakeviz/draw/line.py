"""Draw a line series on an Axes."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.style.line import LineStyle
from lakeviz.style.base import AxKind, stamp_ax


def draw_line(
    ax: plt.Axes,
    x: np.ndarray | pd.Series,
    y: np.ndarray | pd.Series,
    *,
    style: LineStyle = LineStyle(),
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.plot(
        x, y,
        color=style.color,
        linewidth=style.linewidth,
        linestyle=style.linestyle,
        marker=style.marker,
        markersize=style.markersize,
        markerfacecolor=style.markerfacecolor,
        markeredgewidth=style.markeredgewidth,
        alpha=style.alpha,
        label=style.label,
        zorder=style.zorder,
    )