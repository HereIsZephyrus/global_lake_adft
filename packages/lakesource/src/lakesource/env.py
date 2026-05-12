"""Unified environment loading for the lake analysis workspace.

Provides ``load_env()`` which loads the root ``.env`` file and
``ensure_env_loaded()`` as the standard one-shot entry point.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

_ENV_LOADED = False


def _find_dotenv() -> Path:
    """Find the .env file to load.

    Resolution order:
    1. ``LAKE_ENV_FILE`` environment variable (absolute or relative)
    2. ``find_dotenv(usecwd=True)`` (auto-search upward from CWD)
    3. ``<cwd>/.env``
    4. ``<package_root>/packages/lakesource/.env`` (legacy fallback)
    """
    env_file = os.environ.get("LAKE_ENV_FILE")
    if env_file:
        return Path(env_file)

    try:
        from dotenv import find_dotenv
        found = find_dotenv(usecwd=True)
        if found:
            return Path(found)
    except ImportError:
        pass

    cwd_candidate = Path.cwd() / ".env"
    if cwd_candidate.exists():
        return cwd_candidate

    legacy = (
        Path(__file__).resolve().parent.parent.parent.parent / "lakesource" / ".env"
    )
    return legacy


def load_env(dotenv_path: Path | str | None = None, override: bool = False) -> None:
    """Load environment variables from the workspace ``.env`` file.

    Args:
        dotenv_path: Path to the .env file.  Defaults to auto-discovery.
        override: If True, overwrite existing environment variables.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        log.debug("python-dotenv not installed; skipping .env loading")
        return

    path = Path(dotenv_path) if dotenv_path is not None else _find_dotenv()
    if path.exists():
        load_dotenv(dotenv_path=path, override=override)
        log.debug("Loaded environment from %s", path)
    else:
        log.debug(".env file not found at %s; skipping", path)


def ensure_env_loaded(dotenv_path: Path | str | None = None, override: bool = False) -> None:
    """Load the workspace env file once per process."""
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    load_env(dotenv_path=dotenv_path, override=override)
    _ENV_LOADED = True


def config_dir() -> Path:
    """Return the YAML configuration directory path.

    Reads ``LAKE_CONFIG_DIR`` env var; defaults to ``<cwd>/config``.
    """
    raw = os.environ.get("LAKE_CONFIG_DIR", "")
    if raw:
        return Path(raw)
    return Path.cwd() / "config"
