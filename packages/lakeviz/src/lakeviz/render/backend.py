"""Web-oriented matplotlib backend setup for headless environments."""

from __future__ import annotations

import matplotlib

from lakeviz.style.presets import Theme


def setup_web_backend() -> None:
    """Configure matplotlib for web/API use: Agg backend + CJK font support.

    Call once at application startup (e.g. FastAPI lifespan) before any
    figure is created.  Idempotent — repeated calls are safe.
    """
    matplotlib.use("Agg")
    Theme.apply()