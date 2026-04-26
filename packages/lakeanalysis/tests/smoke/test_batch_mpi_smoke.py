"""MPI smoke tests for the batch engine.

Tests the Manager/Worker MPI protocol with small chunk sizes and multiple
workers to cover:
- Worker chunk range isolation (no overlap between workers)
- Manager IO scheduling (no deadlock from double-enqueue)
- End-to-end persist verification

Run with:
    pytest tests/smoke/test_batch_mpi_smoke.py -v -s

Requires:
- ``mpi4py`` installed
- ``mpiexec`` available on PATH
- Env vars: SERIES_DB, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT
"""

from __future__ import annotations

import os
import subprocess
import sys

import pytest


@pytest.fixture(scope="session")
def mpi_available():
    try:
        from mpi4py import MPI  # noqa: F401
    except ImportError:
        pytest.skip("mpi4py not installed")
    try:
        subprocess.run(
            ["mpiexec", "--version"],
            capture_output=True,
            timeout=10,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("mpiexec not available")


def _run_mpi_batch(script_name: str, algorithm: str, id_start: int, id_end: int,
                   np: int, chunk_size: int, timeout: int = 300) -> subprocess.CompletedProcess:
    script = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "scripts", script_name,
    )
    script = os.path.abspath(script)

    cmd = [
        "mpiexec",
        "--oversubscribe",
        "-np", str(np),
        sys.executable,
        script,
        "--chunk-size", str(chunk_size),
        "--limit-id", str(id_end),
        "--id-start", str(id_start),
        "--id-end", str(id_end),
    ]

    env = {**os.environ, "LOG_LEVEL": "DEBUG"}

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    return result


@pytest.mark.usefixtures("mpi_available")
def test_mpi_quantile_smoke(id_range, sample_hylak_ids, cleanup_algorithm_rows, series_conn):
    """Launch quantile batch via ``mpiexec -np 3`` with small chunks.

    Uses 3 workers + 1 manager to stress-test the IO scheduling protocol.
    Small chunk size (3) forces multiple chunks per worker, exercising
    the Manager's read/write trigger dispatch loop.
    """
    id_start, id_end = id_range
    cleanup_algorithm_rows.register("quantile")

    result = _run_mpi_batch(
        "run_quantile.py", "quantile",
        id_start, id_end,
        np=4, chunk_size=3,
    )

    assert result.returncode == 0, (
        f"mpiexec failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    hid_list = ",".join(str(h) for h in sample_hylak_ids)
    with series_conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM quantile_run_status "
            f"WHERE hylak_id IN ({hid_list}) AND status = 'done'"
        )
        done_count = int(cur.fetchone()[0])
    assert done_count >= 1, f"Expected at least 1 done status row, got {done_count}"


@pytest.mark.usefixtures("mpi_available")
def test_mpi_chunk_range_isolation(id_range, sample_hylak_ids, cleanup_algorithm_rows, series_conn):
    """Verify each MPI worker only processes its assigned ID range.

    With 3 workers and chunk_size=2, each worker gets a distinct
    [start, end) assignment. No lake should be processed twice.
    """
    id_start, id_end = id_range
    cleanup_algorithm_rows.register("quantile")

    result = _run_mpi_batch(
        "run_quantile.py", "quantile",
        id_start, id_end,
        np=4, chunk_size=2,
    )

    assert result.returncode == 0, (
        f"mpiexec failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    hid_list = ",".join(str(h) for h in sample_hylak_ids)
    with series_conn.cursor() as cur:
        cur.execute(
            f"SELECT hylak_id, COUNT(*) as cnt FROM quantile_run_status "
            f"WHERE hylak_id IN ({hid_list}) GROUP BY hylak_id HAVING COUNT(*) > 1"
        )
        duplicates = cur.fetchall()
    assert len(duplicates) == 0, (
        f"Duplicate run_status rows found (workers overlapped): {duplicates}"
    )


@pytest.mark.usefixtures("mpi_available")
def test_mpi_no_deadlock_on_write(id_range, sample_hylak_ids, cleanup_algorithm_rows, series_conn):
    """Verify Manager does not deadlock when dispatching TRIGGER_WRITE.

    The old bug: Manager enqueued worker to write_queue twice (once on
    CALCULATING, once on PENDING(prev=CALCULATING)), causing a second
    TRIGGER_WRITE that no Worker would recv, deadlocking the system.

    This test uses io_budget=1 to force serialized IO, which maximizes
    the chance of hitting the double-enqueue path.
    """
    id_start, id_end = id_range
    cleanup_algorithm_rows.register("quantile")

    script = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "scripts", "run_quantile.py",
    )
    script = os.path.abspath(script)

    cmd = [
        "mpiexec",
        "--oversubscribe",
        "-np", "4",
        sys.executable,
        script,
        "--chunk-size", "2",
        "--limit-id", str(id_end),
        "--id-start", str(id_start),
        "--id-end", str(id_end),
        "--io-budget", "1",
    ]

    env = {**os.environ, "LOG_LEVEL": "DEBUG"}

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )

    assert result.returncode == 0, (
        f"mpiexec failed or timed out (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    hid_list = ",".join(str(h) for h in sample_hylak_ids)
    with series_conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM quantile_run_status "
            f"WHERE hylak_id IN ({hid_list}) AND status = 'done'"
        )
        done_count = int(cur.fetchone()[0])
    assert done_count >= 1, f"Expected at least 1 done status row, got {done_count}"
