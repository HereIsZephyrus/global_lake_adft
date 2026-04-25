"""Draw reference lines (horizontal, vertical, diagonal) on an Axes."""

from __future__ import annotations

import matplotlib.pyplot as plt

from lakeviz.style.reference import ReferenceLineStyle
from lakeviz.style.base import AxKind, stamp_ax


def draw_axhline(
    ax: plt.Axes,
    y: float,
    *,
    style: ReferenceLineStyle = ReferenceLineStyle(),
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.axhline(
        y,
        color=style.color,
        linestyle=style.linestyle,
        linewidth=style.linewidth,
        alpha=style.alpha,
        label=style.label,
    )


def draw_axvline(
    ax: plt.Axes,
    x: float,
    *,
    style: ReferenceLineStyle = ReferenceLineStyle(),
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.axvline(
        x,
        color=style.color,
        linestyle=style.linestyle,
        linewidth=style.linewidth,
        alpha=style.alpha,
        label=style.label,
    )


def draw_diagonal(
    ax: plt.Axes,
    *,
    color: str = "grey",
    linestyle: str = "--",
    linewidth: float = 1.0,
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    xlim = ax.get_xlim()
    ylim = ax.get_ylim()
    lo = min(xlim[0], ylim[0])
    hi = max(xlim[1], ylim[1])
    ax.plot([lo, hi], [lo, hi], linestyle=linestyle, color=color, linewidth=linewidth)