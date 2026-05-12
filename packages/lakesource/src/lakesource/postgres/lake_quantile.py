"""Database operations for quantile (monthly anomaly) tables."""

from __future__ import annotations

import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _ensure_quantile_labels_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    year                INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    water_area          DOUBLE PRECISION,
    monthly_climatology DOUBLE PRECISION,
    anomaly             DOUBLE PRECISION,
    q_low               DOUBLE PRECISION,
    q_high              DOUBLE PRECISION,
    extreme_label       TEXT,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, year, month)
);
""").format(table=sql.Identifier(tc.series_table("quantile_labels")))


def _ensure_quantile_extremes_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id            INTEGER      NOT NULL,
    year                INTEGER      NOT NULL,
    month               INTEGER      NOT NULL,
    event_type          TEXT         NOT NULL,
    water_area          DOUBLE PRECISION,
    monthly_climatology DOUBLE PRECISION,
    anomaly             DOUBLE PRECISION,
    threshold           DOUBLE PRECISION,
    computed_at         TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id, year, month, event_type)
);
""").format(table=sql.Identifier(tc.series_table("quantile_extremes")))


def _ensure_quantile_abrupt_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    from_year         INTEGER      NOT NULL,
    from_month        INTEGER      NOT NULL,
    to_year           INTEGER      NOT NULL,
    to_month          INTEGER      NOT NULL,
    transition_type   TEXT         NOT NULL,
    from_anomaly      DOUBLE PRECISION,
    to_anomaly        DOUBLE PRECISION,
    from_label        TEXT,
    to_label          TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (
        hylak_id,
        from_year,
        from_month,
        to_year,
        to_month,
        transition_type
    )
);
""").format(table=sql.Identifier(tc.series_table("quantile_abrupt_transitions")))


def _ensure_quantile_status_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id          INTEGER      NOT NULL,
    chunk_start       INTEGER,
    chunk_end         INTEGER,
    status            TEXT         NOT NULL,
    error_message     TEXT,
    computed_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (hylak_id)
);
""").format(table=sql.Identifier(tc.series_table("quantile_run_status")))


def _upsert_quantile_labels_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, year, month,
    water_area, monthly_climatology, anomaly,
    q_low, q_high, extreme_label, computed_at
) VALUES (
    %(hylak_id)s, %(year)s, %(month)s,
    %(water_area)s, %(monthly_climatology)s, %(anomaly)s,
    %(q_low)s, %(q_high)s, %(extreme_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    water_area          = EXCLUDED.water_area,
    monthly_climatology = EXCLUDED.monthly_climatology,
    anomaly             = EXCLUDED.anomaly,
    q_low               = EXCLUDED.q_low,
    q_high              = EXCLUDED.q_high,
    extreme_label       = EXCLUDED.extreme_label,
    computed_at         = now();
""").format(
        table=sql.Identifier(tc.series_table("quantile_labels")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in ("hylak_id", "year", "month")
        ),
    )


def _upsert_quantile_extremes_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, year, month, event_type,
    water_area, monthly_climatology, anomaly, threshold, computed_at
) VALUES (
    %(hylak_id)s, %(year)s, %(month)s, %(event_type)s,
    %(water_area)s, %(monthly_climatology)s, %(anomaly)s, %(threshold)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    water_area          = EXCLUDED.water_area,
    monthly_climatology = EXCLUDED.monthly_climatology,
    anomaly             = EXCLUDED.anomaly,
    threshold           = EXCLUDED.threshold,
    computed_at         = now();
""").format(
        table=sql.Identifier(tc.series_table("quantile_extremes")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in ("hylak_id", "year", "month", "event_type")
        ),
    )


def _upsert_quantile_abrupt_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, from_year, from_month, to_year, to_month, transition_type,
    from_anomaly, to_anomaly, from_label, to_label, computed_at
) VALUES (
    %(hylak_id)s, %(from_year)s, %(from_month)s,
    %(to_year)s, %(to_month)s, %(transition_type)s, %(from_anomaly)s, %(to_anomaly)s,
    %(from_label)s, %(to_label)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    from_anomaly = EXCLUDED.from_anomaly,
    to_anomaly   = EXCLUDED.to_anomaly,
    from_label   = EXCLUDED.from_label,
    to_label     = EXCLUDED.to_label,
    computed_at  = now();
""").format(
        table=sql.Identifier(tc.series_table("quantile_abrupt_transitions")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c)
            for c in (
                "hylak_id",
                "from_year",
                "from_month",
                "to_year",
                "to_month",
                "transition_type",
            )
        ),
    )


def _upsert_quantile_status_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (
    hylak_id, chunk_start, chunk_end, status, error_message, computed_at
) VALUES (
    %(hylak_id)s, %(chunk_start)s, %(chunk_end)s, %(status)s, %(error_message)s, now()
)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    chunk_start   = EXCLUDED.chunk_start,
    chunk_end     = EXCLUDED.chunk_end,
    status        = EXCLUDED.status,
    error_message = EXCLUDED.error_message,
    computed_at   = now();
""").format(
        table=sql.Identifier(tc.series_table("quantile_run_status")),
        conflict_cols=sql.SQL(", ").join(
            sql.Identifier(c) for c in ("hylak_id",)
        ),
    )


def _count_quantile_status_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT COUNT(*)
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
  AND status = 'done'
""").format(table=sql.Identifier(tc.series_table("quantile_run_status")))


def _fetch_quantile_status_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
  AND status = 'done'
""").format(table=sql.Identifier(tc.series_table("quantile_run_status")))


def ensure_quantile_tables(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Create monthly transition result and status tables when missing."""
    with conn.cursor() as cur:
        cur.execute(_ensure_quantile_labels_table_sql(table_config))
        cur.execute(_ensure_quantile_extremes_table_sql(table_config))
        cur.execute(_ensure_quantile_abrupt_table_sql(table_config))
        cur.execute(_ensure_quantile_status_table_sql(table_config))
    conn.commit()
    log.debug("Ensured monthly transition tables exist")


def upsert_quantile_labels(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition label rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_quantile_labels_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d quantile_labels row(s)", len(rows))


def upsert_quantile_extremes(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition extreme rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_quantile_extremes_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d quantile_extremes row(s)", len(rows))


def upsert_quantile_abrupt_transitions(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition abrupt transition rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_quantile_abrupt_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d quantile_abrupt_transitions row(s)", len(rows))


def upsert_quantile_run_status(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    commit: bool = True,
    table_config: TableConfig = _default_table_config,
) -> None:
    """Insert or update monthly transition run-status rows."""
    if not rows:
        return
    with conn.cursor() as cur:
        cur.executemany(_upsert_quantile_status_sql(table_config), rows)
    if commit:
        conn.commit()
    log.info("Upserted %d quantile_run_status row(s)", len(rows))


def count_quantile_status_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    """Count monthly transition run-status rows in a hylak_id range."""
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
    }
    with conn.cursor() as cur:
        cur.execute(_count_quantile_status_in_range_sql(table_config), params)
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def fetch_quantile_status_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    """Fetch processed monthly transition hylak_ids in a hylak_id range."""
    params = {
        "chunk_start": chunk_start,
        "chunk_end": chunk_end,
    }
    with conn.cursor() as cur:
        cur.execute(_fetch_quantile_status_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}
