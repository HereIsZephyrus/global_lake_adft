"""Database operations for area_shift_labels table."""
from __future__ import annotations
import logging
import math
import psycopg
from psycopg import sql
from lakesource.table_config import TableConfig
_default_table_config = TableConfig.default()

log = logging.getLogger(__name__)





def _ensure_area_shift_labels_table_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
CREATE TABLE IF NOT EXISTS {table} (
    hylak_id                      INTEGER PRIMARY KEY,
    shift_label                   TEXT NOT NULL,
    udmax                         DOUBLE PRECISION,
    udmax_p_value                 DOUBLE PRECISION,
    udmax_break_index             INTEGER,
    wdmax                         DOUBLE PRECISION,
    wdmax_p_value                 DOUBLE PRECISION,
    wdmax_break_index             INTEGER,
    used_deseasoned               BOOLEAN,
    seasonality_dominance_ratio   DOUBLE PRECISION,
    computed_at                   TIMESTAMPTZ DEFAULT now()
);
""").format(table=sql.Identifier(tc.series_table("area_shift_labels")))

def _upsert_area_shift_labels_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("""
INSERT INTO {table} (hylak_id, shift_label, udmax, udmax_p_value, udmax_break_index,
                     wdmax, wdmax_p_value, wdmax_break_index, used_deseasoned,
                     seasonality_dominance_ratio, computed_at)
VALUES (%(hylak_id)s, %(shift_label)s, %(udmax)s, %(udmax_p_value)s, %(udmax_break_index)s,
        %(wdmax)s, %(wdmax_p_value)s, %(wdmax_break_index)s, %(used_deseasoned)s,
        %(seasonality_dominance_ratio)s, now())
ON CONFLICT ({conflict_cols}) DO UPDATE SET
    shift_label                   = EXCLUDED.shift_label,
    udmax                         = EXCLUDED.udmax,
    udmax_p_value                 = EXCLUDED.udmax_p_value,
    udmax_break_index             = EXCLUDED.udmax_break_index,
    wdmax                         = EXCLUDED.wdmax,
    wdmax_p_value                 = EXCLUDED.wdmax_p_value,
    wdmax_break_index             = EXCLUDED.wdmax_break_index,
    used_deseasoned               = EXCLUDED.used_deseasoned,
    seasonality_dominance_ratio   = EXCLUDED.seasonality_dominance_ratio,
    computed_at                   = now();
""").format(
        table=sql.Identifier(tc.series_table("area_shift_labels")),
        conflict_cols=sql.SQL(", ").join(sql.Identifier(c) for c in ("hylak_id",)),
    )

def _truncate_area_shift_labels_sql(tc: TableConfig) -> sql.Composed:
    return sql.SQL("TRUNCATE {table}").format(
        table=sql.Identifier(tc.series_table("area_shift_labels"))
    )

def ensure_area_shift_labels_table(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_ensure_area_shift_labels_table_sql(table_config))
    conn.commit()
    log.debug("Ensured area_shift_labels table exists")

def _nan_to_none(v: float | int | None) -> int | None:
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v):
            return None
        return int(v)
    if isinstance(v, int):
        return v
    return None

def upsert_area_shift_labels(
    conn: psycopg.Connection,
    rows: list[dict],
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    if not rows:
        return
    table = table_config.series_table("area_shift_labels")
    with conn.cursor() as cur:
        cur.execute(sql.SQL(
            "CREATE TEMP TABLE _tmp_asl (hylak_id INTEGER, shift_label TEXT, "
            "udmax DOUBLE PRECISION, udmax_p_value DOUBLE PRECISION, "
            "udmax_break_index INTEGER, wdmax DOUBLE PRECISION, "
            "wdmax_p_value DOUBLE PRECISION, wdmax_break_index INTEGER, "
            "used_deseasoned BOOLEAN, seasonality_dominance_ratio DOUBLE PRECISION) "
            "ON COMMIT DROP"
        ))
        with cur.copy("COPY _tmp_asl (hylak_id, shift_label, udmax, udmax_p_value, "
                      "udmax_break_index, wdmax, wdmax_p_value, wdmax_break_index, "
                      "used_deseasoned, seasonality_dominance_ratio) FROM STDIN") as copy:
            for r in rows:
                copy.write_row([
                    r["hylak_id"], r["shift_label"],
                    r.get("udmax"), r.get("udmax_p_value"), _nan_to_none(r.get("udmax_break_index")),
                    r.get("wdmax"), r.get("wdmax_p_value"), _nan_to_none(r.get("wdmax_break_index")),
                    r.get("used_deseasoned"), r.get("seasonality_dominance_ratio"),
                ])
        cur.execute(sql.SQL(
            "INSERT INTO {table} (hylak_id, shift_label, udmax, udmax_p_value, "
            "udmax_break_index, wdmax, wdmax_p_value, wdmax_break_index, "
            "used_deseasoned, seasonality_dominance_ratio, computed_at) "
            "SELECT t.hylak_id, t.shift_label, t.udmax, t.udmax_p_value, "
            "t.udmax_break_index, t.wdmax, t.wdmax_p_value, t.wdmax_break_index, "
            "t.used_deseasoned, t.seasonality_dominance_ratio, now() "
            "FROM _tmp_asl t "
            "ON CONFLICT (hylak_id) DO UPDATE SET "
            "shift_label = EXCLUDED.shift_label, "
            "udmax = EXCLUDED.udmax, "
            "udmax_p_value = EXCLUDED.udmax_p_value, "
            "udmax_break_index = EXCLUDED.udmax_break_index, "
            "wdmax = EXCLUDED.wdmax, "
            "wdmax_p_value = EXCLUDED.wdmax_p_value, "
            "wdmax_break_index = EXCLUDED.wdmax_break_index, "
            "used_deseasoned = EXCLUDED.used_deseasoned, "
            "seasonality_dominance_ratio = EXCLUDED.seasonality_dominance_ratio, "
            "computed_at = now()"
        ).format(table=sql.Identifier(table)))
    conn.commit()
    log.info("Upserted %d area_shift_labels row(s)", len(rows))

def truncate_area_shift_labels(
    conn: psycopg.Connection,
    *,
    table_config: TableConfig = _default_table_config,
) -> None:
    with conn.cursor() as cur:
        cur.execute(_truncate_area_shift_labels_sql(table_config))
    conn.commit()
    log.info("Truncated area_shift_labels table")
