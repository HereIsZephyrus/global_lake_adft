"""MPI smoke test for the batch engine.

Runs the quantile algorithm under ``mpiexec -np 2`` to verify the
Manager/Worker MPI protocol works end-to-end with a real database.

This test is **not** executed by default.  Run explicitly with:

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


@pytest.mark.usefixtures("mpi_available")
def test_mpi_quantile_smoke(id_range, sample_hylak_ids, cleanup_algorithm_rows, series_conn):
    """Launch quantile batch via ``mpiexec -np 2`` and verify results."""
    id_start, id_end = id_range
    cleanup_algorithm_rows.register("quantile")

    script = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "scripts",
        "run_quantile.py",
    )
    script = os.path.abspath(script)

    cmd = [
        "mpiexec",
        "-np", "2",
        sys.executable,
        script,
        "--chunk-size", "10000",
        "--limit-id", str(id_end),
        "--id-start", str(id_start),
        "--id-end", str(id_end),
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=300,
        env={**os.environ},
    )

    assert result.returncode == 0, (
        f"mpiexec failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-2000:]}\n"
        f"stderr: {result.stderr[-2000:]}"
    )

    hid_list = ",".join(str(h) for h in sample_hylak_ids)
    with series_conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) FROM quantile_run_status "
            f"WHERE hylak_id IN ({hid_list}) AND status = 'done'"
        )
        done_count = int(cur.fetchone()[0])
    assert done_count >= 1, f"Expected at least 1 done status row, got {done_count}"
