"""MPI smoke tests for the batch engine using the unified lake_adft CLI."""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def mpi_available():
    """Skip the test session if mpi4py or mpiexec are not available."""
    try:
        from mpi4py import MPI  # noqa: F401
    except ImportError:
        pytest.skip("mpi4py not installed")
    try:
        subprocess.run(["mpiexec", "--version"], capture_output=True, timeout=10, check=False)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pytest.skip("mpiexec not available")


def _run_mpi_batch(
    cli_args: list[str],
    id_start: int,
    id_end: int,
    backend: str,
    parquet_data_dir: Path | None = None,
    parquet_output_dir: Path | None = None,
    *,
    np: int,
    chunk_size: int,
    timeout: int = 300,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    filter_name = "full"
    cmd = [
        "mpiexec",
        "--oversubscribe",
        "-np",
        str(np),
        "uv",
        "run",
        "lake_adft",
        *cli_args,
        "--filter",
        filter_name,
        "--chunk-size",
        str(chunk_size),
        "--id-start",
        str(id_start),
        "--id-end",
        str(id_end),
    ]
    if extra_args:
        cmd.extend(extra_args)

    env = {**os.environ, "LOG_LEVEL": "DEBUG", "DATA_BACKEND": backend}
    env["LAKE_FILTER"] = filter_name
    if backend == "parquet" and parquet_data_dir is not None:
        env["PARQUET_DATA_DIR"] = str(parquet_data_dir)
    if backend == "parquet" and parquet_output_dir is not None:
        env["OUTPUT_DIR"] = str(parquet_output_dir)

    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
        check=False,
    )


def _verify_done_count(
    backend: str,
    source_config,
    parquet_output_dir: Path | None,
    id_start: int,
    id_end: int,
    algorithm: str = "quantile",
) -> int:
    table_name = f"{algorithm}_run_status"
    if backend == "parquet":
        import pandas as pd

        assert parquet_output_dir is not None
        table_path = parquet_output_dir / f"{table_name}.parquet"
        if not table_path.exists():
            return 0
        df = pd.read_parquet(table_path)
        mask = (df["hylak_id"] >= id_start) & (df["hylak_id"] < id_end) & (df["status"] == "done")
        return int(mask.sum())

    import psycopg

    params = source_config.connection_params(source_config.series_db_name or "lakecentroid")
    conn = psycopg.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM {table_name} WHERE hylak_id >= %s AND hylak_id < %s AND status = 'done'",
                (id_start, id_end),
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _verify_result_table_count(
    backend: str,
    source_config,
    parquet_output_dir: Path | None,
    id_start: int,
    id_end: int,
    table_name: str,
) -> int:
    if backend == "parquet":
        import pandas as pd

        assert parquet_output_dir is not None
        table_path = parquet_output_dir / f"{table_name}.parquet"
        if not table_path.exists():
            return 0
        df = pd.read_parquet(table_path)
        mask = (df["hylak_id"] >= id_start) & (df["hylak_id"] < id_end)
        return int(df[mask]["hylak_id"].nunique())

    import psycopg

    params = source_config.connection_params(source_config.series_db_name or "lakecentroid")
    conn = psycopg.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(DISTINCT hylak_id) FROM {table_name} WHERE hylak_id >= %s AND hylak_id < %s",
                (id_start, id_end),
            )
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _parse_success_count(stdout: str) -> int:
    match = re.search(r"success=(\d+)", stdout)
    return int(match.group(1)) if match else -1


def _read_parquet_table(parquet_output_dir: Path, table_name: str):
    import pandas as pd

    table_path = parquet_output_dir / f"{table_name}.parquet"
    if not table_path.exists():
        return None
    return pd.read_parquet(table_path)


@pytest.mark.usefixtures("mpi_available")
def test_mpi_quantile_smoke(backend, id_range, source_config, parquet_data_dir, parquet_output_dir, cleanup):
    id_start, id_end = id_range
    cleanup.register("quantile")

    result = _run_mpi_batch(
        ["eot", "quantile", "--method", "stl"],
        id_start,
        id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        parquet_output_dir=parquet_output_dir,
        np=4,
        chunk_size=3,
    )

    assert result.returncode == 0, f"mpiexec failed (rc={result.returncode}):\nstdout: {result.stdout[-3000:]}\nstderr: {result.stderr[-3000:]}"

    done_count = _verify_done_count(backend, source_config, parquet_output_dir, id_start, id_end)
    n_lakes = id_end - id_start
    assert done_count >= max(1, n_lakes * 0.5), f"Too few done rows: {done_count}/{n_lakes} lakes (expected ≥50%)"

    result_hids = _verify_result_table_count(
        backend,
        source_config,
        parquet_output_dir,
        id_start,
        id_end,
        "quantile_labels",
    )
    assert result_hids >= done_count, f"Result table has fewer lakes ({result_hids}) than done status ({done_count})"


@pytest.mark.usefixtures("mpi_available")
def test_mpi_chunk_range_isolation(backend, id_range, source_config, parquet_data_dir, parquet_output_dir, cleanup):
    id_start, id_end = id_range
    cleanup.register("quantile")

    result = _run_mpi_batch(
        ["eot", "quantile", "--method", "stl"],
        id_start,
        id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        parquet_output_dir=parquet_output_dir,
        np=4,
        chunk_size=2,
    )

    assert result.returncode == 0, f"mpiexec failed (rc={result.returncode}):\nstdout: {result.stdout[-3000:]}\nstderr: {result.stderr[-3000:]}"

    table_name = "quantile_run_status"
    if backend == "parquet":
        import pandas as pd

        table_path = parquet_output_dir / f"{table_name}.parquet"
        if table_path.exists():
            df = pd.read_parquet(table_path)
            mask = (df["hylak_id"] >= id_start) & (df["hylak_id"] < id_end)
            duplicates = df[mask][df[mask].duplicated(subset=["hylak_id"], keep=False)]
            assert duplicates.empty, f"Duplicate run_status rows found: {duplicates['hylak_id'].tolist()}"
        return

    import psycopg

    params = source_config.connection_params(source_config.series_db_name or "lakecentroid")
    conn = psycopg.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT hylak_id, COUNT(*) AS cnt FROM {table_name} WHERE hylak_id >= %s AND hylak_id < %s GROUP BY hylak_id HAVING COUNT(*) > 1",
                (id_start, id_end),
            )
            duplicates = cur.fetchall()
    finally:
        conn.close()
    assert len(duplicates) == 0, f"Duplicate run_status rows found: {duplicates}"


@pytest.mark.usefixtures("mpi_available")
def test_mpi_no_deadlock_on_write(backend, id_range, source_config, parquet_data_dir, parquet_output_dir, cleanup):
    id_start, id_end = id_range
    cleanup.register("quantile")

    result = _run_mpi_batch(
        ["eot", "quantile", "--method", "stl"],
        id_start,
        id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        parquet_output_dir=parquet_output_dir,
        np=4,
        chunk_size=2,
        timeout=120,
    )

    assert result.returncode == 0, f"mpiexec failed or timed out (rc={result.returncode}):\nstdout: {result.stdout[-3000:]}\nstderr: {result.stderr[-3000:]}"

    done_count = _verify_done_count(backend, source_config, parquet_output_dir, id_start, id_end)
    n_lakes = id_end - id_start
    assert done_count >= max(1, n_lakes * 0.5), f"Too few done rows: {done_count}/{n_lakes} lakes (expected ≥50%)"

    result_hids = _verify_result_table_count(
        backend,
        source_config,
        parquet_output_dir,
        id_start,
        id_end,
        "quantile_labels",
    )
    assert result_hids >= done_count, f"Result table has fewer lakes ({result_hids}) than done status ({done_count})"


@pytest.mark.usefixtures("mpi_available")
def test_mpi_pwm_hawkes_smoke(
    backend,
    pwm_hawkes_id_range,
    source_config,
    parquet_data_dir,
    parquet_output_dir,
    cleanup_pwm_hawkes,
):
    id_start, id_end = pwm_hawkes_id_range
    cleanup_pwm_hawkes.register("pwm_hawkes")

    result = _run_mpi_batch(
        ["pwm", "hawkes"],
        id_start,
        id_end,
        backend=backend,
        parquet_data_dir=parquet_data_dir,
        parquet_output_dir=parquet_output_dir,
        np=4,
        chunk_size=1,
    )

    assert result.returncode == 0, (
        f"mpiexec failed (rc={result.returncode}):\n"
        f"stdout: {result.stdout[-3000:]}\n"
        f"stderr: {result.stderr[-3000:]}"
    )

    done_count = _verify_done_count(
        backend,
        source_config,
        parquet_output_dir,
        id_start,
        id_end,
        algorithm="pwm_hawkes",
    )
    assert done_count >= 1, f"Expected at least one done PWM-Hawkes row, got {done_count}"

    result_hids = _verify_result_table_count(
        backend,
        source_config,
        parquet_output_dir,
        id_start,
        id_end,
        "pwm_hawkes_results",
    )
    assert result_hids >= 1, "Expected at least one PWM-Hawkes result row"

    if backend == "parquet":
        assert parquet_output_dir is not None
        results_df = _read_parquet_table(parquet_output_dir, "pwm_hawkes_results")
        segments_df = _read_parquet_table(parquet_output_dir, "pwm_hawkes_segments")
        assert results_df is not None, "Missing pwm_hawkes_results parquet output"
        assert segments_df is not None, "Missing pwm_hawkes_segments parquet output"

        lake_results = results_df[
            (results_df["hylak_id"] >= id_start) & (results_df["hylak_id"] < id_end)
        ]
        lake_segments = segments_df[
            (segments_df["hylak_id"] >= id_start) & (segments_df["hylak_id"] < id_end)
        ]
        assert not lake_results.empty, "No PWM-Hawkes results for smoke lake"
        assert lake_results["error_message"].isna().all(), "Smoke lake should complete without Hawkes error"
        assert {float(q) for q in lake_results["threshold_quantile"].dropna().unique()} == {0.95, 0.99}
        assert not lake_segments.empty, "Expected bidirectional bridge segments to be materialized"
        assert set(lake_segments["segment_type"]).issubset({"transition", "unilateral"})
