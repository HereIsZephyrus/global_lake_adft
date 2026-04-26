"""Base style definitions shared across all draw primitives."""

from __future__ import annotations

from dataclasses import dataclass, replace as dc_replace
from enum import Enum


class AxKind(Enum):
    """Metadata tag declaring what kind of content an Axes holds.

    Every ``draw_*`` function stamps ``ax._ax_kind`` so that the layout
    layer can validate placement (e.g. geographic axes need a projection).
    """

    STATISTICAL = "statistical"
    GEOGRAPHIC = "geographic"


@dataclass(frozen=True)
class DrawStyle:
    color: str = "#afafaf"
    linewidth: float = 1.1
    linestyle: str = "-"
    alpha: float = 1.0
    label: str | None = None
    zorder: int = 2

    def replace(self, **kwargs):
        return dc_replace(self, **kwargs)


@dataclass(frozen=True)
class AxisStyle:
    xlabel: str = ""
    ylabel: str = ""
    title: str = ""
    xlim: tuple[float, float] | None = None
    ylim: tuple[float, float] | None = None
    grid_alpha: float = 0.2
    grid_linestyle: str = ":"
    x_rotation: float = 0
    contour_linewidth: float = 0.5

    def replace(self, **kwargs):
        return dc_replace(self, **kwargs)


@dataclass(frozen=True)
class PanelStyle:
    figsize: tuple[float, float] = (8, 5)
    tight_layout: bool = True
    suptitle: str = ""
    suptitle_fontsize: float = 14


def apply_axis_style(ax, style: AxisStyle) -> None:
    if style.xlabel:
        ax.set_xlabel(style.xlabel)
    if style.ylabel:
        ax.set_ylabel(style.ylabel)
    if style.title:
        ax.set_title(style.title)
    if style.xlim:
        ax.set_xlim(style.xlim)
    if style.ylim:
        ax.set_ylim(style.ylim)
    if style.grid_alpha > 0:
        ax.grid(alpha=style.grid_alpha, linestyle=style.grid_linestyle)
    if style.x_rotation:
        ax.tick_params(axis="x", rotation=style.x_rotation)


def stamp_ax(ax, kind: AxKind) -> None:
    ax._ax_kind = kind


def get_ax_kind(ax) -> AxKind:
    return getattr(ax, "_ax_kind", AxKind.STATISTICAL)