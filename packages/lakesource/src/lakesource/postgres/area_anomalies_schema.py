"""Database operations for area_anomalies table and area_processed view."""
from __future__ import annotations
import logging
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)





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
