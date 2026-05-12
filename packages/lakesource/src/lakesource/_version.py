"""Version information for all packages in the lake analysis workspace.

Reads the installed version via ``importlib.metadata`` so that the version
reported at runtime reflects the installed package, not a hard-coded string.
"""

from __future__ import annotations


def get_version(package: str = "lakesource") -> str:
    """Return the installed version of a lake workspace package.

    Args:
        package: Package name (lakesource, lakeanalysis, or lakeviz).

    Returns:
        Version string or "unknown" if the package is not installed.
    """
    try:
        from importlib.metadata import version
    except ImportError:
        return "unknown"
    try:
        return version(package)
    except Exception:
        return "unknown"


def log_versions(logger_fn=None) -> None:
    """Log all workspace package versions via *logger_fn*.

    If *logger_fn* is None, prints to stdout.
    """
    emit = logger_fn or print
    emit("=== Lake Analysis Workspace Versions ===")
    emit(f"  lakesource    v{get_version('lakesource')}")
    emit(f"  lakeanalysis  v{get_version('lakeanalysis')}")
    try:
        emit(f"  lakeviz       v{get_version('lakeviz')}")
    except Exception:
        pass
