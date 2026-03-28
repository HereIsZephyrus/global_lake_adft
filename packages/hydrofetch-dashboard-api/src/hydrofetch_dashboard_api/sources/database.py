"""PostgreSQL ingest and database size statistics sources."""

from __future__ import annotations

import os
from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from typing import Generator

import psycopg
from psycopg import sql


@dataclass
class DBIngestStats:
    available: bool
    message: str
    table_name: str
    total_rows: int = 0
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
    """Read DB connection params from environment variables.

    Supports both HYDROFETCH_DB_* and DASHBOARD_DB_* prefixes, in that order.
    """
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
def _connection() -> Generator[psycopg.Connection, None, None]:
    with closing(psycopg.connect(**_get_conn_params())) as conn:
        yield conn


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
    """Query per-table sizes, optionally restricted to named tables."""

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


def load_ingest_stats(
    table_name: str,
    days: int = 30,
    recent_limit: int = 20,
    recent_scan_limit: int = 100,
) -> DBIngestStats:
    """Query lightweight ingest statistics from PostgreSQL."""

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
                    sql.SQL("SELECT COUNT(*) FROM {t}").format(
                        t=sql.Identifier(table_name)
                    )
                )
                total_rows = cur.fetchone()[0]

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
                        SELECT hylak_id, date::text, ingested_at::text
                        FROM {t}
                        ORDER BY ingested_at DESC NULLS LAST
                        LIMIT %s
                        """
                    ).format(t=sql.Identifier(table_name)),
                    (max(recent_limit, recent_scan_limit),),
                )
                recent_scan_rows = cur.fetchall()
                latest_at = next(
                    (r[2] for r in recent_scan_rows if r[2] is not None),
                    None,
                )
                recent_rows = [
                    {"hylak_id": r[0], "date": r[1], "ingested_at": r[2]}
                    for r in recent_scan_rows[:recent_limit]
                ]

        return DBIngestStats(
            available=True,
            message="ok",
            table_name=table_name,
            total_rows=int(total_rows or 0),
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


__all__ = [
    "DBIngestStats",
    "DBSizeStats",
    "load_db_size",
    "load_ingest_stats",
    "query_db_total_size",
    "query_table_sizes",
]
