"""PostgreSQL ingest statistics source."""

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
    min_date: str | None = None
    max_date: str | None = None
    latest_ingested_at: str | None = None
    daily_counts: list[dict] = field(default_factory=list)
    recent_rows: list[dict] = field(default_factory=list)


def _get_conn_params() -> dict:
    """Read DB connection params from environment variables.

    Supports both HYDROFETCH_DB_* and DASHBOARD_DB_* prefixes, in that order.
    """
    from dotenv import load_dotenv  # pylint: disable=import-outside-toplevel

    load_dotenv()

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


def load_ingest_stats(
    table_name: str, days: int = 30, recent_limit: int = 20
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
                    sql.SQL(
                        "SELECT COUNT(*), MIN(date), MAX(date), MAX(ingested_at) FROM {t}"
                    ).format(t=sql.Identifier(table_name))
                )
                total_rows, min_date, max_date, latest_at = cur.fetchone()

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
                    (recent_limit,),
                )
                recent_rows = [
                    {"hylak_id": r[0], "date": r[1], "ingested_at": r[2]}
                    for r in cur.fetchall()
                ]

        return DBIngestStats(
            available=True,
            message="ok",
            table_name=table_name,
            total_rows=int(total_rows or 0),
            min_date=str(min_date) if min_date else None,
            max_date=str(max_date) if max_date else None,
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


__all__ = ["DBIngestStats", "load_ingest_stats"]
