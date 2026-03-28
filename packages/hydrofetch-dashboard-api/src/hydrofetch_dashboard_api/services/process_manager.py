"""Manage per-project hydrofetch subprocess lifecycle.

Process state is persisted to a PID file (``hydrofetch.pid``) inside each
project directory so that the FastAPI backend can be restarted at any time
without losing track of running hydrofetch processes.

PID file format (two lines)::

    <pid>
    <pgid>
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

log = logging.getLogger(__name__)

ProcessStatus = Literal["running", "stopped", "finished"]

_REPO_ROOT = Path(__file__).resolve().parents[5]
_HYDROFETCH_ENV_FILE = _REPO_ROOT / "packages" / "hydrofetch" / ".env"

_PID_FILENAME = "hydrofetch.pid"


def _pid_path(project_dir: Path) -> Path:
    return project_dir / _PID_FILENAME


def _write_pid(project_dir: Path, pid: int, pgid: int) -> None:
    _pid_path(project_dir).write_text(f"{pid}\n{pgid}\n")


def _read_pid(project_dir: Path) -> tuple[int, int] | None:
    """Return ``(pid, pgid)`` from the PID file, or ``None`` if absent/corrupt."""
    p = _pid_path(project_dir)
    if not p.is_file():
        return None
    try:
        lines = p.read_text().strip().splitlines()
        return int(lines[0]), int(lines[1])
    except (IndexError, ValueError):
        p.unlink(missing_ok=True)
        return None


def _remove_pid(project_dir: Path) -> None:
    _pid_path(project_dir).unlink(missing_ok=True)


def _is_alive(pid: int) -> bool:
    """Check whether *pid* is still running (signal 0 probe)."""
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class ProcessManager:
    """Thread-safe manager for per-project hydrofetch subprocesses.

    Process tracking is persisted via PID files so that the FastAPI backend
    can be restarted independently of the hydrofetch workers.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._project_dirs: dict[str, Path] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def start(
        self,
        project_id: str,
        gee_project: str,
        credentials_file: str,
        start_date: str,
        end_date: str,
        tile_manifest: str,
        job_dir: Path,
        raw_dir: Path,
        sample_dir: Path,
        log_dir: Path,
        max_concurrent: int = 5,
        db_table: str = "era5_forcing",
    ) -> None:
        """Spawn a ``hydrofetch era5 --run`` subprocess for *project_id*.

        Raises ``RuntimeError`` if the project is already running.
        """
        project_dir = job_dir.parent

        with self._lock:
            if self.status(project_id) == "running":
                raise RuntimeError(f"Project '{project_id}' is already running")
            self._project_dirs[project_id] = project_dir

        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        log_path = log_dir / f"hydrofetch_{ts}.log"

        token_file = project_dir / "token.json"

        env = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "HYDROFETCH_GEE_PROJECT": gee_project,
            "HYDROFETCH_CREDENTIALS_FILE": credentials_file,
            "HYDROFETCH_TOKEN_FILE": str(token_file),
            "HYDROFETCH_JOB_DIR": str(job_dir),
            "HYDROFETCH_RAW_DIR": str(raw_dir),
            "HYDROFETCH_SAMPLE_DIR": str(sample_dir),
            "HYDROFETCH_MAX_CONCURRENT": str(max_concurrent),
            "HYDROFETCH_LOG_DIR": str(log_dir),
        }

        cmd = [
            "uv", "run",
            "--package", "hydrofetch",
        ]
        if _HYDROFETCH_ENV_FILE.is_file():
            cmd += ["--env-file", str(_HYDROFETCH_ENV_FILE)]
        cmd += [
            "hydrofetch", "era5",
            "--start", start_date,
            "--end", end_date,
            "--tile-manifest", tile_manifest,
            "--sink", "db",
            "--db-table", db_table,
            "--run",
        ]

        log.info(
            "Starting hydrofetch for project=%s: %s → %s",
            project_id,
            " ".join(cmd),
            log_path,
        )

        log_file = log_path.open("w", encoding="utf-8")
        proc = subprocess.Popen(
            cmd,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=str(_REPO_ROOT),
            start_new_session=True,
        )

        pgid = os.getpgid(proc.pid)
        _write_pid(project_dir, proc.pid, pgid)
        log.info(
            "hydrofetch started for project=%s (pid=%d, pgid=%d)",
            project_id, proc.pid, pgid,
        )

    def stop(self, project_id: str) -> None:
        """Terminate the running hydrofetch process group for *project_id*."""
        project_dir = self._resolve_dir(project_id)
        if project_dir is None:
            return

        info = _read_pid(project_dir)
        if info is None:
            return
        pid, pgid = info

        if not _is_alive(pid):
            _remove_pid(project_dir)
            return

        log.info(
            "Stopping hydrofetch for project=%s (pid=%d, pgid=%d)",
            project_id, pid, pgid,
        )

        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            _remove_pid(project_dir)
            return

        import time
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if not _is_alive(pid):
                break
            time.sleep(0.5)
        else:
            log.warning("Grace period expired for project=%s, sending SIGKILL", project_id)
            try:
                os.killpg(pgid, signal.SIGKILL)
            except ProcessLookupError:
                pass

        _remove_pid(project_dir)

    def status(self, project_id: str) -> ProcessStatus:
        """Return the process status for *project_id*.

        Reads the PID file and probes the process — no in-memory state needed.
        """
        project_dir = self._resolve_dir(project_id)
        if project_dir is None:
            return "stopped"

        info = _read_pid(project_dir)
        if info is None:
            return "stopped"
        pid, _pgid = info

        if _is_alive(pid):
            return "running"

        _remove_pid(project_dir)
        return "finished"

    def recover(self, projects_dir: Path | None = None) -> None:
        """Scan all project directories for PID files and re-register them.

        Called on API startup so that ``status()`` / ``stop()`` work for
        processes that were launched before a backend restart.
        """
        if projects_dir is None:
            from hydrofetch_dashboard_api import config  # pylint: disable=import-outside-toplevel
            projects_dir = config.PROJECTS_DIR

        recovered = 0
        stale = 0
        for child in sorted(projects_dir.iterdir()):
            if not child.is_dir():
                continue
            pid_file = child / _PID_FILENAME
            if not pid_file.is_file():
                continue
            project_id = child.name
            info = _read_pid(child)
            if info is None:
                continue
            pid, _pgid = info
            if _is_alive(pid):
                with self._lock:
                    self._project_dirs[project_id] = child
                recovered += 1
                log.info(
                    "Recovered running process for project=%s (pid=%d)",
                    project_id, pid,
                )
            else:
                _remove_pid(child)
                stale += 1

        log.info(
            "ProcessManager.recover(): %d running, %d stale PID files cleaned",
            recovered, stale,
        )

    def all_statuses(self) -> dict[str, ProcessStatus]:
        """Return a status dict for every known project_id."""
        with self._lock:
            ids = list(self._project_dirs.keys())
        return {pid: self.status(pid) for pid in ids}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_dir(self, project_id: str) -> Path | None:
        """Return the project directory, checking in-memory cache first."""
        with self._lock:
            d = self._project_dirs.get(project_id)
        if d is not None:
            return d
        from hydrofetch_dashboard_api import config  # pylint: disable=import-outside-toplevel
        candidate = config.PROJECTS_DIR / project_id
        if candidate.is_dir():
            with self._lock:
                self._project_dirs[project_id] = candidate
            return candidate
        return None


# Module-level singleton shared by the FastAPI app
manager = ProcessManager()

__all__ = ["ProcessManager", "ProcessStatus", "manager"]
