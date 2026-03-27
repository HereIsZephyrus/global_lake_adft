"""Project configuration CRUD backed by the local filesystem.

Each project lives in ``{PROJECTS_DIR}/{project_id}/`` and carries a
``config.json`` file plus subdirectories for jobs, raw GeoTIFFs, samples and
subprocess logs.
"""

from __future__ import annotations

import json
import re
import shutil
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ProjectConfig:
    """Persisted per-project configuration."""

    project_id: str
    project_name: str
    gee_project: str
    credentials_file: str
    start_date: str       # YYYY-MM-DD inclusive
    end_date: str         # YYYY-MM-DD exclusive
    max_concurrent: int = 5
    created_at: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ProjectConfig":
        return cls(
            project_id=data["project_id"],
            project_name=data["project_name"],
            gee_project=data["gee_project"],
            credentials_file=data["credentials_file"],
            start_date=data["start_date"],
            end_date=data["end_date"],
            max_concurrent=int(data.get("max_concurrent", 5)),
            created_at=data.get("created_at", ""),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SAFE_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def _validate_id(project_id: str) -> None:
    if not project_id or not _SAFE_ID_RE.match(project_id):
        raise ValueError(
            f"project_id must be non-empty and contain only alphanumerics, "
            f"hyphens and underscores; got {project_id!r}"
        )


def _config_path(projects_dir: Path, project_id: str) -> Path:
    return projects_dir / project_id / "config.json"


def resolve_paths(projects_dir: Path, project_id: str) -> dict[str, Path]:
    """Return absolute paths for the project's data subdirectories."""
    base = (projects_dir / project_id).resolve()
    return {
        "project_dir": base,
        "job_dir": base / "jobs",
        "raw_dir": base / "raw",
        "sample_dir": base / "sample",
        "log_dir": base / "logs",
    }


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def list_projects(projects_dir: Path) -> list[ProjectConfig]:
    """Return all projects found under *projects_dir*, sorted by created_at."""
    projects_dir.mkdir(parents=True, exist_ok=True)
    results: list[ProjectConfig] = []
    for cfg_path in sorted(projects_dir.glob("*/config.json")):
        try:
            data = json.loads(cfg_path.read_text(encoding="utf-8"))
            results.append(ProjectConfig.from_dict(data))
        except Exception:
            pass
    results.sort(key=lambda p: p.created_at)
    return results


def load_project(projects_dir: Path, project_id: str) -> ProjectConfig:
    """Load a single project config.  Raises FileNotFoundError if missing."""
    cfg_path = _config_path(projects_dir, project_id)
    if not cfg_path.is_file():
        raise FileNotFoundError(
            f"Project not found: {project_id} (expected {cfg_path})"
        )
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    return ProjectConfig.from_dict(data)


def create_project(projects_dir: Path, params: dict[str, Any]) -> ProjectConfig:
    """Create a new project directory structure and persist config.json.

    *params* must include: project_name, gee_project, credentials_file,
    start_date, end_date.  Optional: project_id (auto-derived from name if
    absent), max_concurrent.
    """
    # Derive project_id from name if not supplied
    raw_id = str(params.get("project_id", "") or "").strip()
    if not raw_id:
        raw_id = re.sub(r"[^a-zA-Z0-9_\-]", "-", params["project_name"]).strip("-")
        raw_id = re.sub(r"-+", "-", raw_id).lower()

    _validate_id(raw_id)

    cfg_path = _config_path(projects_dir, raw_id)
    if cfg_path.exists():
        raise ValueError(f"Project '{raw_id}' already exists")

    config = ProjectConfig(
        project_id=raw_id,
        project_name=params["project_name"],
        gee_project=params["gee_project"],
        credentials_file=params["credentials_file"],
        start_date=params["start_date"],
        end_date=params["end_date"],
        max_concurrent=int(params.get("max_concurrent", 5)),
    )

    # Create directory tree
    paths = resolve_paths(projects_dir, raw_id)
    for p in paths.values():
        p.mkdir(parents=True, exist_ok=True)

    cfg_path.write_text(
        json.dumps(config.to_dict(), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return config


def delete_project(projects_dir: Path, project_id: str) -> None:
    """Remove the entire project directory.  Raises FileNotFoundError if missing."""
    _validate_id(project_id)
    project_dir = projects_dir / project_id
    if not project_dir.is_dir():
        raise FileNotFoundError(f"Project not found: {project_id}")
    shutil.rmtree(project_dir)


__all__ = [
    "ProjectConfig",
    "create_project",
    "delete_project",
    "list_projects",
    "load_project",
    "resolve_paths",
]
