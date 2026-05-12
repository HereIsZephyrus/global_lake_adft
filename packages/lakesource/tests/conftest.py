"""Integration test fixtures for PostgreSQL-backed tests.

Requires an already-running PostgreSQL instance accessible at
localhost:5432 with user=postgres, password=postgres.

A dedicated ``lake_test`` database is created at session start and
dropped at session end.  All tests are skipped if PostgreSQL is not
reachable.
"""

from __future__ import annotations

import logging
import psycopg
import pytest

from lakesource.config import Backend, SourceConfig

log = logging.getLogger(__name__)

ADMIN_DSN = "host=localhost port=5432 dbname=postgres user=postgres password=postgres"
TEST_DSN = "host=localhost port=5432 dbname=lake_test user=postgres password=postgres"


@pytest.fixture(scope="session")
def _test_db():
    """Create ``lake_test`` at session start, drop it at session end.

    Uses the ``postgres`` default database for admin operations
    because ``CREATE`` / ``DROP DATABASE`` cannot run inside a
    transaction block (autocommit required).
    """
    try:
        admin_conn = psycopg.connect(ADMIN_DSN, connect_timeout=5)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL not reachable at localhost:5432")
    admin_conn.autocommit = True

    try:
        with admin_conn.cursor() as cur:
            cur.execute("DROP DATABASE IF EXISTS lake_test")
            cur.execute("CREATE DATABASE lake_test")
    except psycopg.OperationalError:
        admin_conn.close()
        pytest.skip("Cannot create lake_test — PostgreSQL not reachable")

    yield

    with admin_conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = 'lake_test' AND pid <> pg_backend_pid()"
        )
        cur.execute("DROP DATABASE IF EXISTS lake_test")
    admin_conn.close()


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
def test_conn(monkeypatch, _test_db) -> psycopg.Connection:
    """Raw psycopg connection to test DB. DDL+DML rolled back on teardown."""
    conn = psycopg.connect(TEST_DSN)
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()


@pytest.fixture
def provider(test_db_cfg: SourceConfig, monkeypatch, _test_db):
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
