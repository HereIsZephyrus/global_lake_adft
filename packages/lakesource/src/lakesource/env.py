"""Unified environment loading for lakesource.

Provides ``load_env()`` which loads the ``.env`` file bundled with the
lakesource package (or a custom path).  Call this once at program start
before accessing any environment-dependent configuration.
"""

from __future__ import annotations

import logging
from pathlib import Path

log = logging.getLogger(__name__)

_DEFAULT_ENV_PATH = Path(__file__).resolve().parent.parent.parent.parent / "lakesource" / ".env"


def load_env(dotenv_path: Path | str | None = None, override: bool = False) -> None:
    """Load environment variables from the lakesource ``.env`` file.

    Args:
        dotenv_path: Path to the .env file.  Defaults to
            ``packages/lakesource/.env``.
        override: If True, overwrite existing environment variables.
    """
    try:
        from dotenv import load_dotenv
    except ImportError:
        log.debug("python-dotenv not installed; skipping .env loading")
        return

    path = Path(dotenv_path) if dotenv_path is not None else _DEFAULT_ENV_PATH
    if path.exists():
        load_dotenv(dotenv_path=path, override=override)
        log.debug("Loaded environment from %s", path)
    else:
        log.debug(".env file not found at %s; skipping", path)
