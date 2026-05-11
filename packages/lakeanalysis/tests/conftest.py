"""Shared pytest configuration for lakeanalysis tests."""

import pytest


@pytest.fixture(autouse=True, scope="session")
def _apply_viz_theme():
    """Apply lakeviz Theme so matplotlib uses SimSun for CJK glyphs.

    Without this, plot tests emit ~130 UserWarnings about missing CJK
    glyphs in DejaVu Sans because the font fallback list is not configured.
    """
    from lakeviz.style.presets import Theme

    Theme.apply()
