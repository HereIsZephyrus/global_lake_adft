"""Runtime version helpers sourced from git tags."""

from __future__ import annotations

import subprocess
from pathlib import Path


def get_workspace_version() -> str:
    """Return the most relevant git tag for the workspace.

    Prefer an exact tag on HEAD. Fall back to the most recent reachable tag.
    Return ``unknown`` when no tag is available.
    """
    repo_root = Path(__file__).resolve().parents[4]

    for command in (
        ["git", "describe", "--tags", "--exact-match"],
        ["git", "describe", "--tags", "--abbrev=0"],
        ["git", "rev-parse", "--short", "HEAD"],
    ):
        try:
            result = subprocess.run(
                command,
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )
        except Exception:
            continue
        version = result.stdout.strip()
        if version:
            return version

    return "unknown"


def log_runtime_version(logger_fn=None) -> None:
    """Emit the single runtime version line using the git tag."""
    emit = logger_fn or print
    emit(f"Lake Analysis Runtime Version: {get_workspace_version()}")
