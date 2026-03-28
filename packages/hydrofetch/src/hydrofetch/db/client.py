"""PostgreSQL connection client for hydrofetch.

Connection parameters are read from environment variables (``HYDROFETCH_DB_*``)
so that credentials never appear in job records or source code.
"""

from __future__ import annotations

import logging
from contextlib import closing, contextmanager
from typing import Generator

import psycopg

log = logging.getLogger(__name__)


class DBClient:
    """Thin wrapper around a psycopg connection for the hydrofetch database.

    Instantiate via :meth:`from_config` to read connection parameters from the
    environment, or pass them explicitly for testing.

    Args:
        host: PostgreSQL server host (default ``localhost``).
        port: PostgreSQL server port (default ``5432``).
        dbname: Database name.
        user: Database user.
        password: Database password.
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        dbname: str,
        user: str,
        password: str,
    ) -> None:
        self._params: dict = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    _KEEPALIVE_PARAMS: dict = {
        "connect_timeout": 10,
        "keepalives": 1,
        "keepalives_idle": 10,
        "keepalives_interval": 5,
        "keepalives_count": 3,
        "tcp_user_timeout": 30000,
    }

    def connect(self) -> psycopg.Connection:
        """Return a new psycopg connection.  Caller is responsible for closing."""
        log.debug(
            "Connecting to db=%s host=%s port=%s user=%s",
            self._params["dbname"],
            self._params["host"],
            self._params["port"],
            self._params["user"],
        )
        return psycopg.connect(**self._params, **self._KEEPALIVE_PARAMS)

    @contextmanager
    def connection_context(self) -> Generator[psycopg.Connection, None, None]:
        """Context manager that yields an open connection and closes it on exit."""
        with closing(self.connect()) as conn:
            yield conn

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls) -> "DBClient":
        """Build a :class:`DBClient` from ``HYDROFETCH_DB_*`` environment variables."""
        from hydrofetch.config import get_db_params  # pylint: disable=import-outside-toplevel

        return cls(**get_db_params())

    def __repr__(self) -> str:
        return (
            f"DBClient(host={self._params['host']!r}, "
            f"port={self._params['port']}, "
            f"dbname={self._params['dbname']!r})"
        )


__all__ = ["DBClient"]
