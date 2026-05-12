"""YAML configuration loader for the lake analysis workspace.

Loads config from ``config/*.yaml`` files (adapter, tables, algorithms).
Uses ``pyyaml`` for parsing with safe loader.
"""

from __future__ import annotations

import logging
from typing import Any

import yaml

from .env import config_dir

log = logging.getLogger(__name__)

_LOADED: dict[str, dict[str, Any]] = {}


def load_yaml(name: str) -> dict[str, Any]:
    """Load a single YAML config file.

    Reads from ``<config_dir>/<name>`` and caches the result.
    Returns an empty dict if the file does not exist.

    Args:
        name: Relative path from config_dir, e.g. ``"adapter.yaml"``
              or ``"algorithms/quantile.yaml"``.

    Returns:
        Parsed YAML content as a dict.
    """
    if name in _LOADED:
        return _LOADED[name]

    path = config_dir() / name
    if not path.exists():
        log.debug("Config file not found: %s; using empty defaults", path)
        _LOADED[name] = {}
        return {}

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    _LOADED[name] = data
    log.debug("Loaded config from %s", path)
    return data


def reload() -> None:
    """Clear the config cache, forcing re-read on next access."""
    _LOADED.clear()


def adapter_config() -> dict[str, Any]:
    """Return the adapter config (Layer 4)."""
    return load_yaml("adapter.yaml")


def tables_config() -> dict[str, Any]:
    """Return the table mapping config (Layer 3)."""
    return load_yaml("tables.yaml")


def algorithm_config(name: str) -> dict[str, Any]:
    """Return an algorithm config (Layer 5).

    Args:
        name: Algorithm name, e.g. ``"quantile"``, ``"pwm_extreme"``.

    Returns:
        Algorithm config dict.
    """
    return load_yaml(f"algorithms/{name}.yaml")


def viz_config() -> dict[str, Any]:
    """Return the visualization config."""
    return load_yaml("viz.yaml")
