"""Draw a bar chart on an Axes."""

from __future__ import annotations

from collections.abc import Sequence

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from lakeviz.style.bar import BarStyle
from lakeviz.style.base import AxKind, stamp_ax


def draw_bar(
    ax: plt.Axes,
    labels: Sequence[str],
    values: np.ndarray | pd.Series | list[float],
    *,
    style: BarStyle = BarStyle(),
    colors: list[str] | None = None,
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    bar_colors = colors if colors is not None else [style.color] * len(labels)
    ax.bar(
        labels, values,
        color=bar_colors,
        width=style.width,
        edgecolor=style.edgecolor,
        linewidth=style.linewidth,
        alpha=style.alpha,
        label=style.label,
        zorder=style.zorder,
    )