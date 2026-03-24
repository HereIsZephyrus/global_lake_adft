"""PostgreSQL connection client.

Loads ALTAS_DB, SERIES_DB, DB_USER, DB_PASSWORD from environment (and optional DB_HOST, DB_PORT)
and provides DBClient instances (atlas_db, series_db) for connecting to each database.
"""

import logging
import os
from contextlib import closing, contextmanager
from typing import Generator

import psycopg
from dotenv import load_dotenv

log = logging.getLogger(__name__)


class DBClient:
    """Client for a single database identified by an environment variable name."""

    def __init__(self, db_env_key: str) -> None:
        """Store the env key (e.g. 'ALTAS_DB' or 'SERIES_DB') used to read the database name."""
        self._db_env_key = db_env_key

    def _get_connection_params(self) -> dict:
        load_dotenv()
        db_name = os.environ.get(self._db_env_key)
        db_user = os.environ.get("DB_USER")
        db_password = os.environ.get("DB_PASSWORD")
        if not db_name or not db_user or db_password is None:
            log.error(
                "Missing required env: %s, DB_USER, and DB_PASSWORD must be set",
                self._db_env_key,
            )
            raise ValueError(
                f"{self._db_env_key}, DB_USER, and DB_PASSWORD must be set in environment or .env"
            )
        host = os.environ.get("DB_HOST", "localhost")
        port_str = os.environ.get("DB_PORT", "5432")
        try:
            port = int(port_str)
        except ValueError as err:
            log.error("DB_PORT must be a valid integer, got: %s", port_str)
            raise ValueError(f"DB_PORT must be a valid integer, got: {port_str}") from err
        return {
            "host": host,
            "port": port,
            "dbname": db_name,
            "user": db_user,
            "password": db_password,
        }

    def connect(self) -> psycopg.Connection:
        """Return a new psycopg connection to this client's database.

        Caller must close the connection or use connection_context() for cleanup.

        Returns:
            An open psycopg.Connection.

        Raises:
            ValueError: If required env vars are missing.
            psycopg.OperationalError: If connection fails.
        """
        params = self._get_connection_params()
        log.debug("Connecting to db=%s host=%s port=%s", params["dbname"], params["host"], params["port"])
        return psycopg.connect(**params)

    @contextmanager
    def connection_context(self) -> Generator[psycopg.Connection, None, None]:
        """Context manager that yields a connection and closes it on exit."""
        with closing(self.connect()) as conn:
            yield conn


# Module-level instances for the two databases
atlas_db = DBClient("ALTAS_DB")
series_db = DBClient("SERIES_DB")
