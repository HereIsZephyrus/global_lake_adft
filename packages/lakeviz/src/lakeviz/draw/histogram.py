"""Draw a histogram on an Axes."""

from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.style.histogram import HistogramStyle
from lakeviz.style.base import AxKind, stamp_ax


def draw_histogram(
    ax: plt.Axes,
    values: np.ndarray | pd.Series,
    *,
    style: HistogramStyle = HistogramStyle(),
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.hist(
        values,
        bins=style.bins,
        density=style.density,
        alpha=style.alpha,
        color=style.color,
        edgecolor=style.edgecolor,
        linewidth=style.linewidth,
        label=style.label,
    )