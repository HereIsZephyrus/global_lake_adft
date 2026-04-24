"""Render utilities for serving lakeviz plots over HTTP."""

from .backend import setup_web_backend
from .convert import fig_to_base64

__all__ = ["setup_web_backend", "fig_to_base64"]
