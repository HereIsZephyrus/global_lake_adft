"""PostgreSQL ingest and database size statistics sources."""

from __future__ import annotations

import logging
import os
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from typing import Generator

import psycopg
from psycopg import sql

_log = logging.getLogger(__name__)

_CONNECT_TIMEOUT = 5
_STATEMENT_TIMEOUT_MS = 10_000


@dataclass
class DBIngestStats:
    available: bool
    message: str
    table_name: str
    total_rows: int = 0
    db_connection_count: int = 0
    recent_write_rows_5m: int = 0
    recent_write_rate_per_min: float = 0.0
    latest_ingested_at: str | None = None
    daily_counts: list[dict] = field(default_factory=list)
    recent_rows: list[dict] = field(default_factory=list)


@dataclass
class DBSizeStats:
    available: bool
    message: str
    db_name: str = ""
    db_size_bytes: int = 0
    db_size_pretty: str = ""
    total_updated_at: str | None = None
    tables_updated_at: str | None = None
    tables: list[dict] = field(default_factory=list)


def _get_conn_params() -> dict:
    """Read DB connection params from environment variables."""
    from dotenv import load_dotenv  # pylint: disable=import-outside-toplevel
    from pathlib import Path  # pylint: disable=import-outside-toplevel

    _env_file = Path(__file__).resolve().parents[7] / "packages" / "hydrofetch" / ".env"
    load_dotenv(_env_file)

    def _env(*keys: str) -> str | None:
        for k in keys:
            v = os.environ.get(k, "").strip()
            if v:
                return v
        return None

    dbname = _env("HYDROFETCH_DB", "DASHBOARD_DB")
    user = _env("HYDROFETCH_DB_USER", "DASHBOARD_DB_USER")
    password = _env("HYDROFETCH_DB_PASSWORD", "DASHBOARD_DB_PASSWORD")

    if not (dbname and user and password):
        raise ValueError(
            "DB connection requires HYDROFETCH_DB / HYDROFETCH_DB_USER / HYDROFETCH_DB_PASSWORD"
        )

    host = _env("HYDROFETCH_DB_HOST", "DASHBOARD_DB_HOST") or "localhost"
    port_raw = _env("HYDROFETCH_DB_PORT", "DASHBOARD_DB_PORT") or "5432"
    return {
        "host": host,
        "port": int(port_raw),
        "dbname": dbname,
        "user": user,
        "password": password,
    }


@contextmanager
def _connection(
    statement_timeout_ms: int = _STATEMENT_TIMEOUT_MS,
) -> Generator[psycopg.Connection, None, None]:
    """Open a short-lived connection with connect and statement timeouts."""
    conn = psycopg.connect(
        **_get_conn_params(),
        connect_timeout=_CONNECT_TIMEOUT,
    )
    try:
        conn.execute(
            sql.SQL("SET statement_timeout = {}").format(
                sql.Literal(statement_timeout_ms)
            )
        )
        yield conn
    finally:
        conn.close()


def _zero_table_row(table_name: str) -> dict:
    return {
        "table_name": table_name,
        "total_bytes": 0,
        "total_pretty": "0 bytes",
        "data_bytes": 0,
        "data_pretty": "0 bytes",
        "index_bytes": 0,
        "index_pretty": "0 bytes",
    }


def query_db_total_size(cur: psycopg.Cursor) -> tuple[str, int, str]:
    """Query total size of the current database."""
    cur.execute(
        """
        SELECT current_database(),
               pg_database_size(current_database()),
               pg_size_pretty(pg_database_size(current_database()))
        """
    )
    db_name, db_size_bytes, db_size_pretty = cur.fetchone()
    return db_name, int(db_size_bytes or 0), db_size_pretty or ""


def query_table_sizes(
    cur: psycopg.Cursor,
    table_names: list[str] | None = None,
) -> list[dict]:
    """Query per-table sizes using pg_class (no sequential scans)."""
    if table_names:
        cur.execute(
            """
            SELECT
                c.relname AS table_name,
                pg_total_relation_size(c.oid) AS total_bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_pretty,
                pg_relation_size(c.oid) AS data_bytes,
                pg_size_pretty(pg_relation_size(c.oid)) AS data_pretty,
                pg_total_relation_size(c.oid) - pg_relation_size(c.oid) AS index_bytes,
                pg_size_pretty(pg_total_relation_size(c.oid) - pg_relation_size(c.oid)) AS index_pretty
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relkind IN ('r', 'm', 'p')
              AND c.relname = ANY(%s)
            ORDER BY total_bytes DESC, table_name ASC
            """,
            (table_names,),
        )
        fetched = {
            str(r[0]): {
                "table_name": r[0],
                "total_bytes": r[1],
                "total_pretty": r[2],
                "data_bytes": r[3],
                "data_pretty": r[4],
                "index_bytes": r[5],
                "index_pretty": r[6],
            }
            for r in cur.fetchall()
        }
        return [fetched.get(name, _zero_table_row(name)) for name in table_names]

    cur.execute(
        """
        SELECT
            relname AS table_name,
            pg_total_relation_size(relid) AS total_bytes,
            pg_size_pretty(pg_total_relation_size(relid)) AS total_pretty,
            pg_relation_size(relid) AS data_bytes,
            pg_size_pretty(pg_relation_size(relid)) AS data_pretty,
            pg_total_relation_size(relid) - pg_relation_size(relid) AS index_bytes,
            pg_size_pretty(pg_total_relation_size(relid) - pg_relation_size(relid)) AS index_pretty
        FROM pg_stat_user_tables
        WHERE schemaname = 'public'
        ORDER BY total_bytes DESC
        LIMIT 20
        """
    )
    return [
        {
            "table_name": r[0],
            "total_bytes": r[1],
            "total_pretty": r[2],
            "data_bytes": r[3],
            "data_pretty": r[4],
            "index_bytes": r[5],
            "index_pretty": r[6],
        }
        for r in cur.fetchall()
    ]


def _estimate_row_count(cur: psycopg.Cursor, table_name: str) -> int:
    """Fast row-count estimate from pg_class (no table scan)."""
    cur.execute(
        """
        SELECT GREATEST(c.reltuples, 0)::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relname = %s
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def load_ingest_stats(
    table_name: str,
    days: int = 7,
    recent_limit: int = 20,
) -> DBIngestStats:
    """Query lightweight ingest statistics from PostgreSQL.

    All queries are designed to hit indexes:

    * ``total_rows`` — ``pg_class.reltuples`` (no table scan).
    * ``daily_counts`` — PK ``(hylak_id, date)`` supports GROUP BY date
      with a narrow date range.
    * ``recent_write_rows_5m`` — ``idx … (ingested_at DESC)`` index scan.
    * ``recent_rows`` — same index, ``ORDER BY ingested_at DESC LIMIT``.
    """
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                    """,
                    (table_name,),
                )
                if not cur.fetchone()[0]:
                    return DBIngestStats(
                        available=False,
                        message=f"表 `{table_name}` 不存在",
                        table_name=table_name,
                    )

                cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                    """
                )
                db_connection_count = int(cur.fetchone()[0] or 0)

                total_rows = _estimate_row_count(cur, table_name)

                cur.execute(
                    sql.SQL(
                        """
                        SELECT date::text, COUNT(*) AS row_count
                        FROM {t}
                        WHERE date >= CURRENT_DATE - %s
                        GROUP BY date
                        ORDER BY date DESC
                        """
                    ).format(t=sql.Identifier(table_name)),
                    (days,),
                )
                daily_counts = [
                    {"date": r[0], "row_count": r[1]} for r in cur.fetchall()
                ]

                cur.execute(
                    sql.SQL(
                        """
                        SELECT COUNT(*)
                        FROM {t}
                        WHERE ingested_at >= NOW() - INTERVAL '5 minutes'
                        """
                    ).format(t=sql.Identifier(table_name))
                )
                recent_write_rows_5m = int(cur.fetchone()[0] or 0)
                recent_write_rate_per_min = recent_write_rows_5m / 5.0

                cur.execute(
                    sql.SQL(
                        """
                        SELECT hylak_id, date::text, ingested_at::text
                        FROM {t}
                        WHERE ingested_at IS NOT NULL
                        ORDER BY ingested_at DESC
                        LIMIT %s
                        """
                    ).format(t=sql.Identifier(table_name)),
                    (recent_limit,),
                )
                recent_rows_raw = cur.fetchall()
                latest_at = recent_rows_raw[0][2] if recent_rows_raw else None
                recent_rows = [
                    {"hylak_id": r[0], "date": r[1], "ingested_at": r[2]}
                    for r in recent_rows_raw
                ]

        return DBIngestStats(
            available=True,
            message="ok",
            table_name=table_name,
            total_rows=total_rows,
            db_connection_count=db_connection_count,
            recent_write_rows_5m=recent_write_rows_5m,
            recent_write_rate_per_min=recent_write_rate_per_min,
            latest_ingested_at=str(latest_at) if latest_at else None,
            daily_counts=daily_counts,
            recent_rows=recent_rows,
        )
    except Exception as exc:
        return DBIngestStats(
            available=False,
            message=f"数据库不可用: {exc}",
            table_name=table_name,
        )


def load_db_size(table_names: list[str] | None = None) -> DBSizeStats:
    """Query database total size and per-table sizes from PostgreSQL."""
    try:
        with _connection() as conn:
            with conn.cursor() as cur:
                db_name, db_size_bytes, db_size_pretty = query_db_total_size(cur)
                tables = query_table_sizes(cur, table_names=table_names)

        return DBSizeStats(
            available=True,
            message="ok",
            db_name=db_name,
            db_size_bytes=db_size_bytes,
            db_size_pretty=db_size_pretty,
            tables=tables,
        )
    except Exception as exc:
        return DBSizeStats(
            available=False,
            message=f"数据库不可用: {exc}",
        )


def terminate_zombie_sessions() -> int:
    """Kill PostgreSQL sessions stuck on ``ClientWrite`` (dead client connections).

    Only targets the root-cause blocker (``ClientWrite``).  Downstream
    sessions waiting on ``Lock/relation`` are left alone — they unblock
    automatically once the zombie is terminated.
    """
    try:
        params = _get_conn_params()
    except ValueError:
        _log.debug("DB not configured, skipping zombie cleanup")
        return 0

    try:
        conn = psycopg.connect(**params, connect_timeout=_CONNECT_TIMEOUT)
    except psycopg.OperationalError as exc:
        _log.warning("Could not connect for zombie cleanup: %s", exc)
        return 0

    conn.autocommit = True
    try:
        cur = conn.execute("""
            SELECT pid, wait_event_type, wait_event,
                   now() - state_change AS duration,
                   left(query, 80) AS query
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
              AND state = 'active'
              AND wait_event = 'ClientWrite'
        """)
        targets = cur.fetchall()
        for pid, wt, we, dur, q in targets:
            conn.execute("SELECT pg_terminate_backend(%s)", (pid,))
            _log.info(
                "Terminated zombie session pid=%s (%s/%s, %s): %s",
                pid, wt, we, dur, q,
            )
        if targets:
            _log.info("Zombie cleanup: terminated %d session(s)", len(targets))
        return len(targets)
    finally:
        conn.close()


__all__ = [
    "DBIngestStats",
    "DBSizeStats",
    "load_db_size",
    "load_ingest_stats",
    "query_db_total_size",
    "query_table_sizes",
    "terminate_zombie_sessions",
]
