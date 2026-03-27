"""Optional PostgreSQL statistics for the Hydrofetch dashboard."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from psycopg import sql

from hydrofetch.db.client import DBClient


@dataclass(slots=True)
class DBProgressResult:
    """Database-backed ingest progress summary."""

    available: bool
    message: str
    table_name: str
    total_rows: int = 0
    min_date: str | None = None
    max_date: str | None = None
    latest_ingested_at: str | None = None
    daily_counts: pd.DataFrame | None = None
    recent_rows: pd.DataFrame | None = None


def load_db_progress(table_name: str, days: int = 30, recent_limit: int = 20) -> DBProgressResult:
    """Read lightweight ingest progress stats from PostgreSQL."""

    try:
        db = DBClient.from_config()
        with db.connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                    """,
                    (table_name,),
                )
                exists = bool(cur.fetchone()[0])
                if not exists:
                    return DBProgressResult(
                        available=False,
                        message=f"数据库表 `{table_name}` 不存在。",
                        table_name=table_name,
                    )

                cur.execute(
                    sql.SQL(
                        "SELECT COUNT(*), MIN(date), MAX(date), MAX(ingested_at) FROM {table}"
                    ).format(table=sql.Identifier(table_name))
                )
                total_rows, min_date, max_date, latest_ingested_at = cur.fetchone()

                cur.execute(
                    sql.SQL(
                        """
                        SELECT date, COUNT(*) AS row_count
                        FROM {table}
                        WHERE date >= CURRENT_DATE - %s
                        GROUP BY date
                        ORDER BY date DESC
                        """
                    ).format(table=sql.Identifier(table_name)),
                    (days,),
                )
                daily_counts = pd.DataFrame(cur.fetchall(), columns=["date", "row_count"])

                cur.execute(
                    sql.SQL(
                        """
                        SELECT hylak_id, date, ingested_at
                        FROM {table}
                        ORDER BY ingested_at DESC NULLS LAST
                        LIMIT %s
                        """
                    ).format(table=sql.Identifier(table_name)),
                    (recent_limit,),
                )
                recent_rows = pd.DataFrame(
                    cur.fetchall(),
                    columns=["hylak_id", "date", "ingested_at"],
                )

        return DBProgressResult(
            available=True,
            message="ok",
            table_name=table_name,
            total_rows=int(total_rows or 0),
            min_date=str(min_date) if min_date else None,
            max_date=str(max_date) if max_date else None,
            latest_ingested_at=str(latest_ingested_at) if latest_ingested_at else None,
            daily_counts=daily_counts,
            recent_rows=recent_rows,
        )
    except Exception as exc:
        return DBProgressResult(
            available=False,
            message=f"数据库统计不可用: {exc}",
            table_name=table_name,
        )


__all__ = ["DBProgressResult", "load_db_progress"]
