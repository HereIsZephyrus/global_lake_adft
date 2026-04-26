"""Layout composition layer — creates Figures and distributes Axes.

Inspired by the ``plot_vertial_vars`` workflow:
  1. ``create_figure`` builds a Figure + named Axes from a declarative spec.
  2. The caller fills each Axes via ``draw_*`` / ``domain.*`` functions.
  3. ``save`` or ``show`` finalises the output.

Each Axes is stamped with ``_ax_kind`` (AxKind.STATISTICAL or
AxKind.GEOGRAPHIC) so that the layout layer can validate placement.
"""

from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec

from lakeviz.config import DEFAULT_VIZ_CONFIG
from lakeviz.style.base import AxKind, stamp_ax

log = logging.getLogger(__name__)


def create_figure(
    spec: list[dict],
    *,
    figsize: tuple[float, float] | None = None,
    width_ratios: list[float] | None = None,
    height_ratios: list[float] | None = None,
    projection: str | None = None,
) -> tuple[plt.Figure, dict[str, plt.Axes]]:
    """Create a Figure with named Axes from a declarative spec.

    Parameters
    ----------
    spec:
        List of axis declarations.  Each dict must have ``name``, ``row``,
        ``col``, and may have ``rowspan``, ``colspan``, ``kind``, and
        ``projection``.

        ``kind`` is an :class:`AxKind` that tags the axis as STATISTICAL
        or GEOGRAPHIC.  If omitted, defaults to STATISTICAL.

        ``projection`` overrides the per-axis projection (e.g. ``"cartopy"``
        axes need ``ccrs.Robinson()`` passed here).

    figsize:
        Figure size in inches.  Auto-calculated if omitted.

    width_ratios / height_ratios:
        Passed to :class:`~matplotlib.gridspec.GridSpec`.

    Returns
    -------
    (fig, axes_dict)
    """
    n_rows = max(s["row"] for s in spec) + 1
    n_cols = max(s.get("colspan", 1) + s["col"] for s in spec)

    if figsize is None:
        figsize = (5 * n_cols, 4 * n_rows)

    fig = plt.figure(figsize=figsize)
    gs = gridspec.GridSpec(
        n_rows, n_cols,
        width_ratios=width_ratios,
        height_ratios=height_ratios,
    )

    axes: dict[str, plt.Axes] = {}
    for s in spec:
        rowspan = s.get("rowspan", 1)
        colspan = s.get("colspan", 1)
        subplot_kw: dict = {}
        if "projection" in s:
            subplot_kw["projection"] = s["projection"]
        ax = fig.add_subplot(
            gs[s["row"]:s["row"] + rowspan, s["col"]:s["col"] + colspan],
            **subplot_kw,
        )
        kind = s.get("kind", AxKind.STATISTICAL)
        stamp_ax(ax, kind)
        axes[s["name"]] = ax

    return fig, axes


def save(
    fig: plt.Figure,
    path: Path | str,
    *,
    dpi: int | None = None,
    close: bool = True,
) -> Path:
    """Save a Figure to disk."""
    if dpi is None:
        dpi = DEFAULT_VIZ_CONFIG.default_dpi
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    log.info("Saved figure to %s", path)
    if close:
        plt.close(fig)
    return path