"""Shared fixtures for batch smoke tests.

Provides backend-parametrized fixtures so each test runs against both
PostgreSQL and Parquet backends automatically.

All fixtures are scoped to the ``smoke/`` sub-directory so they do not
interfere with unit tests in the parent ``tests/`` directory.
"""

from __future__ import annotations

import os
from pathlib import Path
import pytest

from lakesource.config import Backend, SourceConfig
from lakesource.env import load_env
from lakeanalysis.batch import build_batch_reader
from lakeanalysis.batch.calculator import CalculatorFactory
from lakeanalysis.batch import LakeTask


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _require_db_env():
    """Skip if PostgreSQL env vars are not configured."""
    dbname = os.environ.get("SERIES_DB")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    if not dbname or not user or password is None:
        pytest.skip("SERIES_DB, DB_USER, DB_PASSWORD env vars required for postgres smoke test")


def _parquet_data_dir() -> Path:
    """Resolve parquet data directory from env vars."""
    data_dir = os.environ.get("SMOKE_PARQUET_DIR") or os.environ.get("LAKE_DATA_DIR")
    if not data_dir:
        pytest.skip("SMOKE_PARQUET_DIR or LAKE_DATA_DIR env var required for parquet smoke test")
    path = Path(data_dir)
    if not path.exists():
        pytest.skip(f"Parquet data dir does not exist: {path}")
    return path


_CLEANUP_TABLES: dict[str, list[str]] = {
    "quantile": [
        "quantile_labels",
        "quantile_extremes",
        "quantile_abrupt_transitions",
        "quantile_run_status",
    ],
    "pwm_extreme": [
        "pwm_extreme_thresholds",
        "pwm_extreme_labels",
        "pwm_extreme_extremes",
        "pwm_extreme_abrupt_transitions",
        "pwm_extreme_run_status",
    ],
    "eot": [
        "eot_results",
        "eot_extremes",
        "eot_run_status",
    ],
}

_PWM_CANDIDATE_STARTS = [2, 6, 14, 35, 63, 68, 483363]
_PWM_CANDIDATE_WINDOW = 10


# ------------------------------------------------------------------
# Backend parametrization
# ------------------------------------------------------------------

@pytest.fixture(params=["parquet"], scope="session")
def backend(request):
    """Parametrize tests across parquet backend only.

    Postgres removed to avoid hang when local PG is unreachable.
    """
    load_env()
    _parquet_data_dir()  # validates availability
    return request.param


@pytest.fixture(scope="session")
def parquet_data_dir():
    """Return the parquet data directory (session-scoped, independent of backend param)."""
    load_env()
    return _parquet_data_dir()


@pytest.fixture(scope="session")
def source_config(backend, parquet_data_dir):
    """Return SourceConfig for the current backend."""
    if backend == "parquet":
        return SourceConfig(backend=Backend.PARQUET, data_dir=parquet_data_dir)
    return SourceConfig(backend=Backend.POSTGRES)


# ------------------------------------------------------------------
# ID range discovery
# ------------------------------------------------------------------

@pytest.fixture(scope="session")
def id_range(backend, source_config, parquet_data_dir):
    """Discover an id range suitable for quantile/eot on the current backend.

    Returns (start, end) covering a small set of lakes with area_quality data.
    """
    if backend == "parquet":
        from lakesource.parquet.client import DuckDBClient

        client = DuckDBClient(data_dir=parquet_data_dir)
        try:
            df = client.query_df(
                "SELECT hylak_id FROM area_quality ORDER BY hylak_id LIMIT 10"
            )
        except Exception:
            pytest.skip("Cannot read area_quality from parquet data dir")
        if df.empty:
            pytest.skip("No area_quality data in parquet")
        return int(df["hylak_id"].min()), int(df["hylak_id"].max()) + 1
    else:
        import psycopg

        params = source_config.connection_params(source_config.series_db_name or "lakecentroid")
        conn = psycopg.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT hylak_id FROM area_quality "
                    "WHERE rs_area_mean IS NOT NULL "
                    "ORDER BY hylak_id LIMIT 10"
                )
                ids = [int(row[0]) for row in cur.fetchall()]
        finally:
            conn.close()
        if not ids:
            pytest.skip("No area_quality data in postgres")
        return min(ids), max(ids) + 1


@pytest.fixture(scope="session")
def pwm_id_range(backend, source_config):
    """Discover an id range suitable for pwm_extreme on the current backend.

    Brute-force trial: iterate candidate start IDs, run PWM on each lake,
    return first range with at least 1 success.
    Candidate list shared across backends (data is同源).
    """
    reader = build_batch_reader(source_config)
    calculator = CalculatorFactory.create("pwm_extreme")

    for start in _PWM_CANDIDATE_STARTS:
        end = start + _PWM_CANDIDATE_WINDOW
        lake_map = reader.fetch_lake_area_chunk(start, end)
        frozen_map = reader.fetch_frozen_year_months_chunk(start, end)
        success_ids: list[int] = []
        for hylak_id in sorted(lake_map):
            task = LakeTask(
                hylak_id=hylak_id,
                series_df=lake_map[hylak_id],
                frozen_year_months=frozenset(frozen_map.get(hylak_id, set())),
                extra=None,
            )
            try:
                calculator.run(task)
                success_ids.append(hylak_id)
            except Exception:
                continue
        if success_ids:
            return min(success_ids), max(success_ids) + 1
    pytest.skip(f"No lakes satisfy PWM smoke requirements on {backend} backend")


# ------------------------------------------------------------------
# Cleanup
# ------------------------------------------------------------------

@pytest.fixture()
def cleanup(backend, source_config, id_range, parquet_data_dir):
    """Post-test cleanup: remove algorithm output rows/files.

    Usage::

        def test_foo(cleanup):
            cleanup.register("quantile")
            # ... run engine ...
    """
    registered: list[str] = []

    class _Cleanup:
        def register(self, algorithm: str) -> None:
            registered.append(algorithm)

    yield _Cleanup()

    id_start, id_end = id_range
    for algo in registered:
        tables = _CLEANUP_TABLES.get(algo, [])
        if backend == "parquet":
            for table in tables:
                table_path = parquet_data_dir / f"{table}.parquet"
                table_path.unlink(missing_ok=True)
        else:
            import psycopg

            params = source_config.connection_params(
                source_config.series_db_name or "lakecentroid"
            )
            conn = psycopg.connect(**params)
            try:
                for table in tables:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"DELETE FROM {table} "
                            f"WHERE hylak_id >= %s AND hylak_id < %s",
                            (id_start, id_end),
                        )
                    conn.commit()
            finally:
                conn.close()


@pytest.fixture()
def cleanup_pwm(backend, source_config, pwm_id_range, parquet_data_dir):
    """Post-test cleanup for PWM tests (uses pwm_id_range)."""
    registered: list[str] = []

    class _Cleanup:
        def register(self, algorithm: str) -> None:
            registered.append(algorithm)

    yield _Cleanup()

    id_start, id_end = pwm_id_range
    for algo in registered:
        tables = _CLEANUP_TABLES.get(algo, [])
        if backend == "parquet":
            for table in tables:
                table_path = parquet_data_dir / f"{table}.parquet"
                table_path.unlink(missing_ok=True)
        else:
            import psycopg

            params = source_config.connection_params(
                source_config.series_db_name or "lakecentroid"
            )
            conn = psycopg.connect(**params)
            try:
                for table in tables:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"DELETE FROM {table} "
                            f"WHERE hylak_id >= %s AND hylak_id < %s",
                            (id_start, id_end),
                        )
                    conn.commit()
            finally:
                conn.close()


# ------------------------------------------------------------------
# PostgreSQL connection (for MPI test verification)
# ------------------------------------------------------------------

@pytest.fixture(scope="session")
def series_conn(backend, source_config):
    """PostgreSQL connection for MPI test verification.

    Only available when backend=postgres.
    """
    if backend != "postgres":
        pytest.skip("series_conn only available for postgres backend")
    import psycopg

    params = source_config.connection_params(source_config.series_db_name or "lakecentroid")
    conn = psycopg.connect(**params)
    yield conn
    conn.close()
