"""Centralised logging setup for hydrofetch.

Environment variables (read after :func:`hydrofetch.config.load_env`):

* ``HYDROFETCH_LOG_LEVEL`` — ``DEBUG``, ``INFO``, etc. Ignored when CLI ``-v`` is used.
* ``LOG_LEVEL`` — fallback if ``HYDROFETCH_LOG_LEVEL`` is unset.
* ``HYDROFETCH_LOG_DIR`` — directory for session log files (default: ``logs`` under the
  current working directory). Set to ``none``, ``-``, ``false``, or ``0`` for console only.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path


def _resolve_log_level(verbose: bool) -> int:
    if verbose:
        return logging.DEBUG
    for key in ("HYDROFETCH_LOG_LEVEL", "LOG_LEVEL"):
        raw = os.environ.get(key, "").strip().upper()
        if raw:
            return getattr(logging, raw, logging.INFO)
    return logging.INFO


def _file_log_dir() -> Path | None:
    raw = os.environ.get("HYDROFETCH_LOG_DIR", "logs").strip()
    if not raw or raw.lower() in ("none", "-", "false", "0"):
        return None
    return Path(raw).expanduser().resolve()


def setup_logging(*, verbose: bool = False) -> Path | None:
    """Configure the root logger with console and optional file handlers.

    Removes any existing root handlers first so repeated calls (e.g. in tests) do not
    duplicate output.

    Args:
        verbose: When True, force ``DEBUG`` on the root logger and handlers.

    Returns:
        Path to the log file when file logging is enabled, else ``None``.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()

    level = _resolve_log_level(verbose)
    root.setLevel(level)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(formatter)
    root.addHandler(console)

    log_path: Path | None = None
    log_dir = _file_log_dir()
    if log_dir is not None:
        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_path = log_dir / f"hydrofetch_{stamp}.log"
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)

    return log_path


__all__ = ["setup_logging"]
