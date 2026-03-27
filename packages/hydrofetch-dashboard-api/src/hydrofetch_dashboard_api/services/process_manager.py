"""Manage per-project hydrofetch subprocess lifecycle.

A single ``ProcessManager`` instance (module-level singleton ``manager``) is
shared across the FastAPI app.  It spawns one ``hydrofetch era5 --run``
subprocess per project and tracks its state.

The hydrofetch CLI itself persists job records to disk, so if the Dashboard API
is restarted the job progress is not lost — the user can call ``start`` again
and hydrofetch will skip already-completed jobs (idempotent).
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

log = logging.getLogger(__name__)

ProcessStatus = Literal["running", "stopped", "finished"]

_REPO_ROOT = Path(__file__).resolve().parents[5]
_HYDROFETCH_ENV_FILE = _REPO_ROOT / "packages" / "hydrofetch" / ".env"


class ProcessManager:
    """Thread-safe manager for per-project hydrofetch subprocesses."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._procs: dict[str, subprocess.Popen] = {}

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
        with self._lock:
            existing = self._procs.get(project_id)
            if existing is not None and existing.poll() is None:
                raise RuntimeError(f"Project '{project_id}' is already running")

        log_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%S")
        log_path = log_dir / f"hydrofetch_{ts}.log"

        # Per-project token file lives alongside config.json
        project_dir = job_dir.parent
        token_file = project_dir / "token.json"

        env = {
            **os.environ,
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
        )

        with self._lock:
            self._procs[project_id] = proc

    def stop(self, project_id: str) -> None:
        """Terminate the running subprocess for *project_id*.

        No-op if the project is not running.
        """
        with self._lock:
            proc = self._procs.get(project_id)
        if proc is None or proc.poll() is not None:
            return
        log.info("Stopping hydrofetch for project=%s (pid=%d)", project_id, proc.pid)
        proc.terminate()
        try:
            proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()

    def status(self, project_id: str) -> ProcessStatus:
        """Return the process status for *project_id*.

        ``"running"``  – subprocess is alive.
        ``"finished"`` – subprocess exited on its own (all jobs done).
        ``"stopped"``  – subprocess was never started, or was terminated.
        """
        with self._lock:
            proc = self._procs.get(project_id)
        if proc is None:
            return "stopped"
        rc = proc.poll()
        if rc is None:
            return "running"
        if rc == 0:
            return "finished"
        return "stopped"

    def recover(self) -> None:
        """Called on API startup.

        Nothing to auto-restart — subprocesses do not survive a Python process
        restart.  This is a no-op placeholder so callers don't need to check.
        """
        log.info("ProcessManager.recover(): no in-memory processes to restore")

    def all_statuses(self) -> dict[str, ProcessStatus]:
        """Return a status dict for every known project_id."""
        with self._lock:
            ids = list(self._procs.keys())
        return {pid: self.status(pid) for pid in ids}


# Module-level singleton shared by the FastAPI app
manager = ProcessManager()

__all__ = ["ProcessManager", "ProcessStatus", "manager"]
