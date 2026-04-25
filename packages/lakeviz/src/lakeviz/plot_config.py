"""Backward-compatible re-export — use ``lakeviz.style.presets.Theme.apply`` instead."""

from __future__ import annotations

from .style.presets import Theme


def setup_chinese_font() -> None:
    Theme.apply()