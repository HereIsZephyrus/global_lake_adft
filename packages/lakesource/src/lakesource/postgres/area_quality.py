"""Area quality and anomaly table operations."""

from __future__ import annotations

import logging

import psycopg
from psycopg import sql

from lakesource.table_config import TableConfig

log = logging.getLogger(__name__)

_default_table_config = TableConfig.default()


def _fetch_atlas_area_chunk_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id, lake_area AS atlas_area
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("lake_info")))


def _ensure_area_quality_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def _upsert_area_quality_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (hylak_id, rs_area_mean, rs_area_median, atlas_area, computed_at)
VALUES (%(hylak_id)s, %(rs_area_mean)s, %(rs_area_median)s, %(atlas_area)s, now())
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    rs_area_mean   = EXCLUDED.rs_area_mean,
    rs_area_median = EXCLUDED.rs_area_median,
    atlas_area     = EXCLUDED.atlas_area,
    computed_at    = now();
""").format(
        table=sql.Identifier(tc.series_table("area_quality")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )


def _ensure_area_anomalies_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id       INTEGER PRIMARY KEY,
    rs_area_mean   DOUBLE PRECISION,
    rs_area_median DOUBLE PRECISION,
    atlas_area     DOUBLE PRECISION,
    anomaly_flags  INTEGER DEFAULT 0,
    computed_at    TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("area_anomalies")))


def _ensure_area_processed_view_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE OR REPLACE VIEW area_processed AS
    SELECT hylak_id FROM {area_quality}
    UNION ALL
    SELECT hylak_id FROM {area_anomalies};
""").format(
        area_quality=sql.Identifier(tc.series_table("area_quality")),
        area_anomalies=sql.Identifier(tc.series_table("area_anomalies")),
    )


def _upsert_area_anomalies_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags, computed_at)
VALUES (%(hylak_id)s, %(rs_area_mean)s, %(rs_area_median)s, %(atlas_area)s, %(anomaly_flags)s, now())
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    rs_area_mean   = EXCLUDED.rs_area_mean,
    rs_area_median = EXCLUDED.rs_area_median,
    atlas_area     = EXCLUDED.atlas_area,
    anomaly_flags  = EXCLUDED.anomaly_flags,
    computed_at    = now();
""").format(
        table=sql.Identifier(tc.series_table("area_anomalies")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )


def _fetch_anomaly_hylak_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id FROM {table}
""").format(table=sql.Identifier(tc.series_table("area_anomalies")))


def _move_area_quality_to_anomalies_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {area_anomalies} (hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags, computed_at)
SELECT q.hylak_id, q.rs_area_mean, q.rs_area_median, q.atlas_area, 0, now()
FROM {area_quality} q
WHERE q.hylak_id = ANY(%(id_list)s)
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    rs_area_mean   = EXCLUDED.rs_area_mean,
    rs_area_median = EXCLUDED.rs_area_median,
    atlas_area     = EXCLUDED.atlas_area,
    computed_at    = now();
""").format(
        area_anomalies=sql.Identifier(tc.series_table("area_anomalies")),
        area_quality=sql.Identifier(tc.series_table("area_quality")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )


def _delete_area_quality_by_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
DELETE FROM {table}
WHERE hylak_id = ANY(%(id_list)s)
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def _fetch_area_quality_hylak_ids_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
ORDER BY hylak_id
LIMIT %(lim)s OFFSET %(off)s
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def _fetch_area_quality_ids_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT hylak_id
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
ORDER BY hylak_id
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def _count_area_quality_in_range_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
SELECT COUNT(*)
FROM {table}
WHERE hylak_id >= %(chunk_start)s::bigint AND hylak_id < %(chunk_end)s::bigint
""").format(table=sql.Identifier(tc.series_table("area_quality")))


def fetch_atlas_area_chunk(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> dict[int, float]:
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_atlas_area_chunk_sql(table_config), params)
        rows = cur.fetchall()
    return {int(r[0]): float(r[1]) if r[1] is not None else 0.0 for r in rows}


def ensure_area_quality_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_area_quality_table_sql(table_config))
    conn.commit()
    log.debug("Ensured area_quality table exists")


def upsert_area_quality(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    if not rows:
        return
    table = table_config.series_table("area_quality")
    with conn.cursor() as cur:
        cur.execute(sql.SQL(
            "CREATE TEMP TABLE _tmp_aq (hylak_id INTEGER, rs_area_mean DOUBLE PRECISION, "
            "rs_area_median DOUBLE PRECISION, atlas_area DOUBLE PRECISION) ON COMMIT DROP"
        ))
        with cur.copy("COPY _tmp_aq (hylak_id, rs_area_mean, rs_area_median, atlas_area) FROM STDIN") as copy:
            for r in rows:
                copy.write_row([
                    r["hylak_id"], r["rs_area_mean"],
                    r["rs_area_median"], r["atlas_area"],
                ])
        cur.execute(sql.SQL(
            "INSERT INTO {table} (hylak_id, rs_area_mean, rs_area_median, atlas_area, computed_at) "
            "SELECT t.hylak_id, t.rs_area_mean, t.rs_area_median, t.atlas_area, now() "
            "FROM _tmp_aq t "
            "ON CONFLICT (hylak_id) DO UPDATE SET "
            "rs_area_mean = EXCLUDED.rs_area_mean, "
            "rs_area_median = EXCLUDED.rs_area_median, "
            "atlas_area = EXCLUDED.atlas_area, "
            "computed_at = now()"
        ).format(table=sql.Identifier(table)))
    conn.commit()
    log.info("Upserted %d area_quality row(s)", len(rows))


def ensure_area_anomalies_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_area_anomalies_table_sql(table_config))
        cur.execute(_ensure_area_processed_view_sql(table_config))
    conn.commit()
    log.debug("Ensured area_anomalies table and area_processed view exist")


def upsert_area_anomalies(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    if not rows:
        return
    table = table_config.series_table("area_anomalies")
    with conn.cursor() as cur:
        cur.execute(sql.SQL(
            "CREATE TEMP TABLE _tmp_aa (hylak_id INTEGER, rs_area_mean DOUBLE PRECISION, "
            "rs_area_median DOUBLE PRECISION, atlas_area DOUBLE PRECISION, "
            "anomaly_flags INTEGER) ON COMMIT DROP"
        ))
        with cur.copy("COPY _tmp_aa (hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags) FROM STDIN") as copy:
            for r in rows:
                copy.write_row([
                    r["hylak_id"], r["rs_area_mean"], r["rs_area_median"],
                    r["atlas_area"], r["anomaly_flags"],
                ])
        cur.execute(sql.SQL(
            "INSERT INTO {table} (hylak_id, rs_area_mean, rs_area_median, atlas_area, anomaly_flags, computed_at) "
            "SELECT t.hylak_id, t.rs_area_mean, t.rs_area_median, t.atlas_area, t.anomaly_flags, now() "
            "FROM _tmp_aa t "
            "ON CONFLICT (hylak_id) DO UPDATE SET "
            "rs_area_mean = EXCLUDED.rs_area_mean, "
            "rs_area_median = EXCLUDED.rs_area_median, "
            "atlas_area = EXCLUDED.atlas_area, "
            "anomaly_flags = EXCLUDED.anomaly_flags, "
            "computed_at = now()"
        ).format(table=sql.Identifier(table)))
    conn.commit()
    log.info("Upserted %d area_anomalies row(s)", len(rows))


def move_area_quality_to_anomalies(
    conn: psycopg.Connection,
    id_list: list[int],
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    if not id_list:
        return 0
    params = {"id_list": [int(hylak_id) for hylak_id in id_list]}
    with conn.cursor() as cur:
        cur.execute(_move_area_quality_to_anomalies_sql(table_config), params)
        cur.execute(_delete_area_quality_by_ids_sql(table_config), params)
        moved = int(cur.rowcount or 0)
    conn.commit()
    log.info("Moved %d hylak_id(s) from area_quality to area_anomalies", moved)
    return moved


def fetch_area_quality_hylak_ids(
    conn: psycopg.Connection,
    *,
    limit: int,
    offset: int = 0,
    table_config: TableConfig = _default_table_config,
) -> list[int]:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if offset < 0:
        raise ValueError("offset must be >= 0")
    params = {"lim": limit, "off": offset}
    with conn.cursor() as cur:
        cur.execute(_fetch_area_quality_hylak_ids_sql(table_config), params)
        rows = cur.fetchall()
    return [int(r[0]) for r in rows]


def fetch_area_quality_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> set[int]:
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_fetch_area_quality_ids_in_range_sql(table_config), params)
        rows = cur.fetchall()
    return {int(row[0]) for row in rows}


def count_area_quality_hylak_ids_in_range(
    conn: psycopg.Connection,
    chunk_start: int,
    chunk_end: int,
    *,
    table_config: TableConfig = _default_table_config,
) -> int:
    params = {"chunk_start": chunk_start, "chunk_end": chunk_end}
    with conn.cursor() as cur:
        cur.execute(_count_area_quality_in_range_sql(table_config), params)
        row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0
