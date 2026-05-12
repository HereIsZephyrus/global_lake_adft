"""MPI smoke tests for the batch engine.

Tests the Manager/Worker MPI protocol with small chunk sizes and multiple
workers to cover:
- Worker chunk range isolation (no overlap between workers)
- Manager IO scheduling (no deadlock from double-enqueue)
- End-to-end persist verification

Tests run against both PostgreSQL and Parquet backends via fixture parametrization.

Run with:
    uv run pytest tests/smoke/test_batch_mpi_smoke.py -v

Requires:
- ``mpi4py`` installed
- ``mpiexec`` available on PATH
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


def _uv_python() -> str:
    """Return the uv-managed Python interpreter path for MPI subprocesses."""
    return str(Path(__file__).resolve().parents[4] / ".venv" / "bin" / "python")


@pytest.fixture(scope="session")
def mpi_available():
    """Skip the test session if mpi4py or mpiexec are not available."""
    try:
        from mpi4py import MPI  # noqa: F401
    except ImportError:
        pytest.skip("mpi4py not installed")
    try:
        subprocess.run(
            ["mpiexec", "--version"],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("mpiexec not available")


def _run_mpi_batch(
    script_name: str,
    id_start: int,
    id_end: int,
    backend: str,
    parquet_data_dir: Path | None = None,
    *,
    np: int,
    chunk_size: int,
    timeout: int = 300,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    script = os.path.join(
        os.path.dirname(__file__),
        "..", "..", "scripts", script_name,
    )
    script = os.path.abspath(script)

    cmd = [
        "mpiexec",
        "--oversubscribe",
        "-np", str(np),
        _uv_python(),
        script,
        "--chunk-size", str(chunk_size),
        "--limit-id", str(id_end),
        "--id-start", str(id_start),
        "--id-end", str(id_end),
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = {**os.environ, "LOG_LEVEL": "DEBUG", "DATA_BACKEND": backend}
    if backend == "parquet" and parquet_data_dir is not None:
        env["PARQUET_DATA_DIR"] = str(parquet_data_dir)
        env["LAKE_DATA_DIR"] = str(parquet_data_dir)

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        check=False,
    )
    return result


def _verify_done_count(
    backend: str,
    source_config,
    parquet_data_dir: Path | None,
    id_start: int,
    id_end: int,
    algorithm: str = "quantile",
) -> int:
    """Verify done records exist in the appropriate backend."""
    table_name = f"{algorithm}_run_status"
    if backend == "parquet":
        import pandas as pd

        assert parquet_data_dir is not None
        table_path = parquet_data_dir / f"{table_name}.parquet"
        if not table_path.exists():
            return 0
        df = pd.read_parquet(table_path)
        mask = (
            (df["hylak_id"] >= id_start)
            & (df["hylak_id"] < id_end)
            & (df["status"] == "done")
        )
        return int(mask.sum())
    else:
        import psycopg

        params = source_config.connection_params(
            source_config.series_db_name or "lakecentroid"
        )
        conn = psycopg.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT COUNT(*) FROM {table_name} "
                    f"WHERE hylak_id >= %s AND hylak_id < %s AND status = 'done'",
                    (id_start, id_end),
                )
                return int(cur.fetchone()[0])
        finally:
            conn.close()


# ------------------------------------------------------------------
# MPI smoke tests
# ------------------------------------------------------------------

@pytest.mark.usefixtures("mpi_available")
def test_mpi_quantile_smoke(backend, id_range, source_config, parquet_data_dir, cleanup):
    """Launch quantile batch via ``mpiexec -np 4`` with small chunks.

    Uses 3 workers + 1 manager to stress-test the IO scheduling protocol.
    """
    id_start, id_end = id_range
    cleanup.register("quantile")

    result = _run_mpi_batch(
        "run_quantile.py",
        id_start, id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        np=4, chunk_size=3,
    )

    assert result.returncode == 0, (
        f"mpiexec failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    done_count = _verify_done_count(
        backend, source_config, parquet_data_dir, id_start, id_end
    )
    assert done_count >= 1, f"Expected at least 1 done status row, got {done_count}"


@pytest.mark.usefixtures("mpi_available")
def test_mpi_chunk_range_isolation(backend, id_range, source_config, parquet_data_dir, cleanup):
    """Verify each MPI worker only processes its assigned ID range.

    With 3 workers and chunk_size=2, each worker gets a distinct
    [start, end) assignment. No lake should be processed twice.
    """
    id_start, id_end = id_range
    cleanup.register("quantile")

    result = _run_mpi_batch(
        "run_quantile.py",
        id_start, id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        np=4, chunk_size=2,
    )

    assert result.returncode == 0, (
        f"mpiexec failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    # Verify no duplicate processing
    table_name = "quantile_run_status"
    if backend == "parquet":
        import pandas as pd

        table_path = parquet_data_dir / f"{table_name}.parquet"
        if table_path.exists():
            df = pd.read_parquet(table_path)
            mask = (df["hylak_id"] >= id_start) & (df["hylak_id"] < id_end)
            filtered = df[mask]
            duplicates = filtered[filtered.duplicated(subset=["hylak_id"], keep=False)]
            assert duplicates.empty, (
                f"Duplicate run_status rows found (workers overlapped): "
                f"{duplicates['hylak_id'].tolist()}"
            )
    else:
        import psycopg

        params = source_config.connection_params(
            source_config.series_db_name or "lakecentroid"
        )
        conn = psycopg.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT hylak_id, COUNT(*) as cnt FROM {table_name} "
                    f"WHERE hylak_id >= %s AND hylak_id < %s "
                    f"GROUP BY hylak_id HAVING COUNT(*) > 1",
                    (id_start, id_end),
                )
                duplicates = cur.fetchall()
        finally:
            conn.close()
        assert len(duplicates) == 0, (
            f"Duplicate run_status rows found (workers overlapped): {duplicates}"
        )


@pytest.mark.usefixtures("mpi_available")
def test_mpi_no_deadlock_on_write(backend, id_range, source_config, parquet_data_dir, cleanup):
    """Verify Manager does not deadlock when dispatching triggers.

    Uses io_budget=1 to force serialized IO, which maximizes
    the chance of hitting the double-enqueue path.
    """
    id_start, id_end = id_range
    cleanup.register("quantile")

    result = _run_mpi_batch(
        "run_quantile.py",
        id_start, id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        np=4, chunk_size=2,
        timeout=120,
        extra_args=["--io-budget", "1"],
    )

    assert result.returncode == 0, (
        f"mpiexec failed or timed out (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    done_count = _verify_done_count(
        backend, source_config, parquet_data_dir, id_start, id_end
    )
    assert done_count >= 1, f"Expected at least 1 done status row, got {done_count}"
