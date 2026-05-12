"""Integration test fixtures for PostgreSQL-backed tests.

Requires an already-running PostgreSQL instance accessible at
localhost:5432 with user=postgres, password=postgres, and a
database named ``lake_test``.

Skipped if the database is not reachable.
"""

from __future__ import annotations

import logging
import psycopg
import pytest

from lakesource.config import Backend, SourceConfig

log = logging.getLogger(__name__)

TEST_DSN = "host=localhost port=5432 dbname=lake_test user=postgres password=postgres"


def _pg_reachable() -> bool:
    try:
        conn = psycopg.connect(TEST_DSN, connect_timeout=3)
        conn.close()
        return True
    except psycopg.OperationalError:
        return False


@pytest.fixture(scope="session")
def _test_db_available() -> bool:
    if not _pg_reachable():
        pytest.skip("PostgreSQL not reachable at localhost:5432/lake_test")
    return True


@pytest.fixture(scope="session")
def test_db_cfg() -> SourceConfig:
    return SourceConfig(
        backend=Backend.POSTGRES,
        db_host="localhost",
        db_port=5432,
        db_user="postgres",
        db_password="postgres",
        series_db_name="lake_test",
    )


@pytest.fixture
def test_conn(monkeypatch, _test_db_available) -> psycopg.Connection:
    """Raw psycopg connection to test DB. DDL+DML rolled back on teardown."""
    conn = psycopg.connect(TEST_DSN)
    conn.autocommit = False  # rollback-enabled transaction
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def provider(test_db_cfg: SourceConfig, monkeypatch, _test_db_available):
    """PostgresLakeProvider wired to test database via env monkeypatching.

    Redirects the ``series_db`` module-level singleton so all
    repository connection factories resolve to the test database.
    """
    monkeypatch.setenv("SERIES_DB", "lake_test")
    monkeypatch.setenv("DB_USER", "postgres")
    monkeypatch.setenv("DB_PASSWORD", "postgres")
    monkeypatch.setenv("DB_HOST", "localhost")
    monkeypatch.setenv("DB_PORT", "5432")

    from lakesource.provider.postgres_provider import PostgresLakeProvider
    return PostgresLakeProvider(test_db_cfg)
