"""Shared utilities (logging setup, plotting defaults)."""

from __future__ import annotations

from typing import Any

from hydrofetch.utils.logger import setup_logging

__all__ = ["setup_chinese_font", "setup_logging"]


def __getattr__(name: str) -> Any:
    if name == "setup_chinese_font":
        from hydrofetch.utils.plot_config import setup_chinese_font  # noqa: PLC0415

        return setup_chinese_font
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
