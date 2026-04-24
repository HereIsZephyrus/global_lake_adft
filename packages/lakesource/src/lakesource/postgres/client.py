"""PostgreSQL connection client.

Supports two construction modes:

1. **From SourceConfig** (preferred): ``DBClient.from_config(config, db_name)``
   uses the connection parameters stored in ``SourceConfig``.

2. **From environment** (legacy): ``DBClient("ALTAS_DB")`` reads the database
   name from the named environment variable and other params from ``DB_USER``,
   ``DB_PASSWORD``, ``DB_HOST``, ``DB_PORT``.
"""

from __future__ import annotations

import logging
import os
from contextlib import closing, contextmanager
from typing import TYPE_CHECKING, Generator

import psycopg

if TYPE_CHECKING:
    from ..config import SourceConfig

log = logging.getLogger(__name__)


class DBClient:
    """Client for a single PostgreSQL database."""

    def __init__(
        self,
        db_name: str | None = None,
        *,
        db_env_key: str | None = None,
        host: str = "localhost",
        port: int = 5432,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        if db_name is None and db_env_key is None:
            raise ValueError("Either db_name or db_env_key must be provided")
        self._db_name = db_name
        self._db_env_key = db_env_key
        self._host = host
        self._port = port
        self._user = user
        self._password = password

    @classmethod
    def from_config(cls, config: SourceConfig, db_name: str) -> DBClient:
        """Create a DBClient from a SourceConfig and an explicit database name.

        Args:
            config: SourceConfig with connection parameters.
            db_name: Database name to connect to.

        Raises:
            ValueError: If config is missing db_user or db_password.
        """
        if not config.db_user or config.db_password is None:
            raise ValueError(
                "SourceConfig must have db_user and db_password set "
                "(via constructor or .env)"
            )
        return cls(
            db_name=db_name,
            host=config.resolved_db_host,
            port=config.resolved_db_port,
            user=config.db_user,
            password=config.db_password,
        )

    def _get_connection_params(self) -> dict:
        if self._db_name is not None and self._user is not None and self._password is not None:
            return {
                "host": self._host,
                "port": self._port,
                "dbname": self._db_name,
                "user": self._user,
                "password": self._password,
            }

        from ..env import load_env
        load_env()

        db_name = self._db_name or os.environ.get(self._db_env_key or "")
        db_user = self._user or os.environ.get("DB_USER")
        db_password = self._password if self._password is not None else os.environ.get("DB_PASSWORD")
        if not db_name or not db_user or db_password is None:
            key = self._db_env_key or "db_name"
            log.error("Missing required: %s, DB_USER, and DB_PASSWORD", key)
            raise ValueError(
                f"{key}, DB_USER, and DB_PASSWORD must be set in environment or .env"
            )
        host = self._host if self._host != "localhost" else os.environ.get("DB_HOST", "localhost")
        port_str = os.environ.get("DB_PORT", str(self._port))
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
            ValueError: If required connection params are missing.
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


atlas_db = DBClient(db_env_key="ALTAS_DB")
series_db = DBClient(db_env_key="SERIES_DB")
