"""Runtime version helpers sourced from the workspace commitizen version."""

from __future__ import annotations

from pathlib import Path


def get_workspace_version() -> str:
    """Return the single workspace version managed by commitizen.

    Falls back to ``unknown`` when the workspace ``pyproject.toml`` cannot be
    resolved or does not expose ``tool.commitizen.version``.
    """
    try:
        import tomllib
    except ModuleNotFoundError:
        return "unknown"

    pyproject = Path(__file__).resolve().parents[4] / "pyproject.toml"
    try:
        with pyproject.open("rb") as fh:
            data = tomllib.load(fh)
    except Exception:
        return "unknown"

    return str(data.get("tool", {}).get("commitizen", {}).get("version", "unknown"))


def log_runtime_version(logger_fn=None) -> None:
    """Emit the single runtime version line using the commitizen version."""
    emit = logger_fn or print
    emit(f"Lake Analysis Runtime Version: v{get_workspace_version()}")
