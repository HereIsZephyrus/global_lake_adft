"""Figure-to-bytes conversion utilities for web/API rendering."""

from __future__ import annotations

import base64
import io

import matplotlib.pyplot as plt

from lakeviz.config import DEFAULT_VIZ_CONFIG


def fig_to_base64(fig: plt.Figure, *, dpi: int | None = None, format: str = "png") -> str:
    """Render a *Figure* to a base64-encoded image string.

    Parameters
    ----------
    fig:
        The matplotlib Figure to render.
    dpi:
        Resolution in dots per inch.  Defaults to ``VizConfig.default_dpi``.
    format:
        Image format passed to ``savefig`` (``"png"``, ``"svg"``, …).

    Returns
    -------
    str
        Base64-encoded image data (no data-URI prefix).
    """
    if dpi is None:
        dpi = DEFAULT_VIZ_CONFIG.default_dpi
    buf = io.BytesIO()
    fig.savefig(buf, format=format, dpi=dpi, bbox_inches="tight")
    buf.seek(0)
    encoded = base64.b64encode(buf.read()).decode("utf-8")
    plt.close(fig)
    return encoded
