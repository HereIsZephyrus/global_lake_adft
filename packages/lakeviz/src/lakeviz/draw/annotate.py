"""Draw annotations on an Axes."""

from __future__ import annotations

import matplotlib.pyplot as plt

from lakeviz.style.base import AxKind, stamp_ax


def draw_annotate_point(
    ax: plt.Axes,
    text: str,
    xy: tuple[float, float],
    *,
    xytext: tuple[float, float] = (4, 6),
    fontsize: int = 8,
    color: str = "tomato",
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    ax.annotate(
        text, xy,
        xytext=xytext,
        textcoords="offset points",
        fontsize=fontsize,
        color=color,
    )


def draw_text_box(
    ax: plt.Axes,
    text: str,
    *,
    x: float = 0.97,
    y: float = 0.05,
    ha: str = "right",
    va: str = "bottom",
    fontsize: int = 9,
    bbox: dict | None = None,
) -> None:
    stamp_ax(ax, AxKind.STATISTICAL)
    if bbox is None:
        bbox = dict(boxstyle="round,pad=0.4", facecolor="white", alpha=0.8)
    ax.text(
        x, y, text,
        transform=ax.transAxes,
        ha=ha, va=va,
        fontsize=fontsize,
        bbox=bbox,
    )