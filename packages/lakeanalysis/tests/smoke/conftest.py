"""Shared fixtures for batch smoke tests.

Provides:
- PostgreSQL backend fixtures (requires DB env vars)
- Parquet backend fixtures (requires SMOKE_PARQUET_DIR or LAKE_DATA_DIR env var)
- Per-algorithm cleanup of rows inserted during smoke tests

All fixtures are scoped to the ``smoke/`` sub-directory so they do not
interfere with unit tests in the parent ``tests/`` directory.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def _require_db_env():
    dbname = os.environ.get("SERIES_DB")
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    if not dbname or not user or password is None:
        pytest.skip("SERIES_DB, DB_USER, DB_PASSWORD env vars required for smoke test")
    return {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": dbname,
        "user": user,
        "password": password,
    }


# ------------------------------------------------------------------
# PostgreSQL fixtures
# ------------------------------------------------------------------

@pytest.fixture(scope="session")
def series_conn():
    psycopg = pytest.importorskip("psycopg")
    from lakesource.env import load_env
    load_env()
    params = _require_db_env()
    conn = psycopg.connect(**params)
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def sample_hylak_ids(series_conn):
    """Pick a small set of real lake IDs that have area_quality data."""
    with series_conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT hylak_id
            FROM area_quality
            WHERE rs_area_mean IS NOT NULL
            ORDER BY hylak_id
            LIMIT 5
            """
        )
        ids = [int(row[0]) for row in cur.fetchall()]
    if not ids:
        pytest.skip("No area_quality data in database")
    return ids


@pytest.fixture(scope="session")
def id_range(sample_hylak_ids):
    """Return (start, end) that covers the sample IDs for --limit-id usage."""
    return min(sample_hylak_ids), max(sample_hylak_ids) + 1


@pytest.fixture(scope="session")
def provider():
    """Create a PostgresLakeProvider from env config."""
    from lakesource.config import SourceConfig
    from lakesource.provider import create_provider
    from lakesource.env import load_env
    load_env()
    _require_db_env()
    return create_provider(SourceConfig())


_CLEANUP_TABLES = {
    "quantile": [
        "quantile_labels",
        "quantile_extremes",
        "quantile_abrupt_transitions",
        "quantile_run_status",
    ],
    "pwm_extreme": [
        "pwm_extreme_thresholds",
        "pwm_extreme_run_status",
    ],
    "eot": [
        "eot_results",
        "eot_extremes",
        "eot_run_status",
    ],
}


@pytest.fixture()
def cleanup_algorithm_rows(series_conn, sample_hylak_ids):
    """Register algorithms for post-test row cleanup.

    Usage::

        def test_foo(cleanup_algorithm_rows):
            cleanup_algorithm_rows.register("quantile")
            # ... run engine ...
    """
    registered: list[str] = []

    class _Cleanup:
        def register(self, algorithm: str) -> None:
            registered.append(algorithm)

    cleanup = _Cleanup()

    yield cleanup

    hid_list = ",".join(str(h) for h in sample_hylak_ids)
    for algo in registered:
        for table in _CLEANUP_TABLES.get(algo, []):
            with series_conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM {table} WHERE hylak_id IN ({hid_list})"
                )
            series_conn.commit()


# ------------------------------------------------------------------
# Parquet fixtures (independent of PostgreSQL)
# ------------------------------------------------------------------

@pytest.fixture(scope="session")
def parquet_data_dir():
    """Return the parquet data directory from SMOKE_PARQUET_DIR or LAKE_DATA_DIR env var."""
    from lakesource.env import load_env
    load_env()
    data_dir = os.environ.get("SMOKE_PARQUET_DIR") or os.environ.get("LAKE_DATA_DIR")
    if not data_dir:
        pytest.skip("SMOKE_PARQUET_DIR or LAKE_DATA_DIR env var required for parquet smoke test")
    path = Path(data_dir)
    if not path.exists():
        pytest.skip(f"Parquet data dir does not exist: {path}")
    return path


@pytest.fixture(scope="session")
def parquet_provider(parquet_data_dir):
    """Create a ParquetLakeProvider pointing at the parquet data directory."""
    from lakesource.config import Backend, SourceConfig
    from lakesource.provider import create_provider
    return create_provider(SourceConfig(
        backend=Backend.PARQUET,
        data_dir=parquet_data_dir,
    ))


@pytest.fixture(scope="session")
def parquet_id_range(parquet_data_dir):
    """Discover an id range suitable for quantile and EOT parquet smoke runs."""
    from lakesource.parquet.client import DuckDBClient

    client = DuckDBClient(data_dir=parquet_data_dir)
    try:
        df = client.query_df(
            """
            SELECT hylak_id
            FROM area_quality
            ORDER BY hylak_id
            LIMIT 10
            """
        )
        if df.empty:
            pytest.skip("No parquet lakes meet defrozen smoke requirements")
        return int(df["hylak_id"].min()), int(df["hylak_id"].max()) + 1
    except Exception:
        pytest.skip("Cannot read area_quality from parquet data dir")


@pytest.fixture(scope="session")
def parquet_pwm_id_range(parquet_data_dir):
    """Discover a parquet id range that is known to satisfy current PWM smoke requirements."""
    from lakesource.config import Backend, SourceConfig
    from lakeanalysis.batch import build_batch_reader
    from lakeanalysis.batch.calculator import CalculatorFactory
    from lakeanalysis.batch.engine import LakeTask

    source_config = SourceConfig(backend=Backend.PARQUET, data_dir=parquet_data_dir)
    reader = build_batch_reader(source_config)
    calculator = CalculatorFactory.create("pwm_extreme")

    candidate_starts = [2, 6, 14, 35, 63, 68, 483363]
    window = 10
    for start in candidate_starts:
        end = start + window
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
    pytest.skip("No parquet lakes satisfy current PWM smoke requirements")
