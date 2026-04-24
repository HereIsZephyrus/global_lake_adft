"""Web-oriented matplotlib backend setup for headless environments."""

from __future__ import annotations

import matplotlib


def setup_web_backend() -> None:
    """Configure matplotlib for web/API use: Agg backend + CJK font support.

    Call once at application startup (e.g. FastAPI lifespan) before any
    figure is created.  Idempotent — repeated calls are safe.
    """
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams["font.sans-serif"] = ["Unifont", "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
